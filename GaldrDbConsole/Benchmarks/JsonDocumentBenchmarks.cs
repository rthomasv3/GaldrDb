using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using GaldrDbEngine.Utilities;
using GaldrDocument = GaldrDbEngine.Json.JsonDocument;
using StjDocument = System.Text.Json.JsonDocument;

namespace GaldrDbConsole.Benchmarks;

/// <summary>
/// Benchmarks comparing GaldrDbEngine.Json.JsonDocument wrapper against System.Text.Json.
/// </summary>
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class JsonDocumentBenchmarks
{
    private byte[] _simpleJsonBytes;
    private byte[] _complexJsonBytes;

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Warmup the writer pool
        JsonWriterPool.Warmup(16);

        // Simple document with a few fields
        string simpleJsonString = """
            {
                "Id": 12345,
                "Name": "John Doe",
                "Age": 30,
                "Email": "john.doe@example.com",
                "IsActive": true
            }
            """;
        _simpleJsonBytes = Encoding.UTF8.GetBytes(simpleJsonString);

        // More complex document with nested objects and arrays
        string complexJsonString = """
            {
                "Id": 99999,
                "Name": "Jane Smith",
                "Age": 28,
                "Email": "jane.smith@example.com",
                "IsActive": true,
                "Balance": 1234.56,
                "Tags": ["developer", "manager", "lead"],
                "Address": {
                    "Street": "123 Main St",
                    "City": "Springfield",
                    "ZipCode": "12345",
                    "Country": "USA"
                },
                "Scores": [95, 87, 92, 88, 91],
                "Metadata": {
                    "CreatedAt": "2024-01-15T10:30:00Z",
                    "UpdatedAt": "2024-06-20T14:45:00Z",
                    "Version": 3
                }
            }
            """;
        _complexJsonBytes = Encoding.UTF8.GetBytes(complexJsonString);
    }

    #region Parse Benchmarks

    [Benchmark(Description = "Galdr Parse Simple (bytes)")]
    public GaldrDocument GaldrParseSimpleBytes()
    {
        return GaldrDocument.Parse(_simpleJsonBytes);
    }

    [Benchmark(Description = "STJ JsonNode Parse Simple (bytes)")]
    public JsonNode StjJsonNodeParseSimpleBytes()
    {
        return JsonNode.Parse(_simpleJsonBytes);
    }

    [Benchmark(Description = "STJ JsonDocument Parse Simple (bytes)")]
    public StjDocument StjDocumentParseSimpleBytes()
    {
        return StjDocument.Parse(_simpleJsonBytes);
    }

    [Benchmark(Description = "Galdr Parse Complex (bytes)")]
    public GaldrDocument GaldrParseComplexBytes()
    {
        return GaldrDocument.Parse(_complexJsonBytes);
    }

    [Benchmark(Description = "STJ JsonNode Parse Complex (bytes)")]
    public JsonNode StjJsonNodeParseComplexBytes()
    {
        return JsonNode.Parse(_complexJsonBytes);
    }

    [Benchmark(Description = "STJ JsonDocument Parse Complex (bytes)")]
    public StjDocument StjDocumentParseComplexBytes()
    {
        return StjDocument.Parse(_complexJsonBytes);
    }

    #endregion

    #region Parse + Read Benchmarks

    [Benchmark(Description = "Galdr Parse+Read Simple")]
    public int GaldrParseReadSimple()
    {
        GaldrDocument doc = GaldrDocument.Parse(_simpleJsonBytes);
        int id = doc.GetInt32("Id");
        string name = doc.GetString("Name");
        int age = doc.GetInt32("Age");
        return id + age + name.Length;
    }

    [Benchmark(Description = "STJ JsonNode Parse+Read Simple")]
    public int StjJsonNodeParseReadSimple()
    {
        JsonNode node = JsonNode.Parse(_simpleJsonBytes);
        int id = node["Id"].GetValue<int>();
        string name = node["Name"].GetValue<string>();
        int age = node["Age"].GetValue<int>();
        return id + age + name.Length;
    }

    [Benchmark(Description = "STJ JsonDocument Parse+Read Simple")]
    public int StjDocumentParseReadSimple()
    {
        using StjDocument doc = StjDocument.Parse(_simpleJsonBytes);
        JsonElement root = doc.RootElement;
        int id = root.GetProperty("Id").GetInt32();
        string name = root.GetProperty("Name").GetString();
        int age = root.GetProperty("Age").GetInt32();
        return id + age + name.Length;
    }

    [Benchmark(Description = "Galdr Parse+Read Complex")]
    public int GaldrParseReadComplex()
    {
        GaldrDocument doc = GaldrDocument.Parse(_complexJsonBytes);
        int id = doc.GetInt32("Id");
        string name = doc.GetString("Name");
        int age = doc.GetInt32("Age");
        decimal balance = doc.GetDecimal("Balance");
        GaldrDocument address = doc.GetObject("Address");
        string city = address.GetString("City");
        return id + age + name.Length + (int)balance + city.Length;
    }

    [Benchmark(Description = "STJ JsonNode Parse+Read Complex")]
    public int StjJsonNodeParseReadComplex()
    {
        JsonNode node = JsonNode.Parse(_complexJsonBytes);
        int id = node["Id"].GetValue<int>();
        string name = node["Name"].GetValue<string>();
        int age = node["Age"].GetValue<int>();
        decimal balance = node["Balance"].GetValue<decimal>();
        string city = node["Address"]["City"].GetValue<string>();
        return id + age + name.Length + (int)balance + city.Length;
    }

    [Benchmark(Description = "STJ JsonDocument Parse+Read Complex")]
    public int StjDocumentParseReadComplex()
    {
        using StjDocument doc = StjDocument.Parse(_complexJsonBytes);
        JsonElement root = doc.RootElement;
        int id = root.GetProperty("Id").GetInt32();
        string name = root.GetProperty("Name").GetString();
        int age = root.GetProperty("Age").GetInt32();
        decimal balance = root.GetProperty("Balance").GetDecimal();
        string city = root.GetProperty("Address").GetProperty("City").GetString();
        return id + age + name.Length + (int)balance + city.Length;
    }

    #endregion

    #region Parse + Mutate + Serialize Benchmarks

    [Benchmark(Description = "Galdr Parse+Mutate+Serialize Simple")]
    public byte[] GaldrParseModifySerializeSimple()
    {
        GaldrDocument doc = GaldrDocument.Parse(_simpleJsonBytes);
        doc.SetString("Name", "Updated Name");
        doc.SetInt32("Age", 31);
        return doc.ToUtf8Bytes();
    }

    [Benchmark(Description = "STJ JsonNode Parse+Mutate+Serialize Simple")]
    public byte[] StjJsonNodeParseModifySerializeSimple()
    {
        JsonNode node = JsonNode.Parse(_simpleJsonBytes);
        node["Name"] = "Updated Name";
        node["Age"] = 31;
        return Encoding.UTF8.GetBytes(node.ToJsonString());
    }

    [Benchmark(Description = "Galdr Parse+Mutate+Serialize Complex")]
    public byte[] GaldrParseModifySerializeComplex()
    {
        GaldrDocument doc = GaldrDocument.Parse(_complexJsonBytes);
        doc.SetString("Name", "Updated Name");
        doc.SetInt32("Age", 29);
        doc.SetDecimal("Balance", 9999.99m);
        GaldrDocument address = doc.GetObject("Address");
        address.SetString("City", "New City");
        return doc.ToUtf8Bytes();
    }

    [Benchmark(Description = "STJ JsonNode Parse+Mutate+Serialize Complex")]
    public byte[] StjJsonNodeParseModifySerializeComplex()
    {
        JsonNode node = JsonNode.Parse(_complexJsonBytes);
        node["Name"] = "Updated Name";
        node["Age"] = 29;
        node["Balance"] = 9999.99m;
        node["Address"]["City"] = "New City";
        return Encoding.UTF8.GetBytes(node.ToJsonString());
    }

    #endregion

    #region Serialization Only Benchmarks

    [Benchmark(Description = "Galdr Serialize Simple")]
    public byte[] GaldrSerializeSimple()
    {
        GaldrDocument doc = new GaldrDocument();
        doc.SetInt32("Id", 12345);
        doc.SetString("Name", "John Doe");
        doc.SetInt32("Age", 30);
        doc.SetString("Email", "john.doe@example.com");
        doc.SetBoolean("IsActive", true);
        return doc.ToUtf8Bytes();
    }

    [Benchmark(Description = "STJ JsonNode Serialize Simple")]
    public byte[] StjJsonNodeSerializeSimple()
    {
        JsonObject obj = new JsonObject
        {
            ["Id"] = 12345,
            ["Name"] = "John Doe",
            ["Age"] = 30,
            ["Email"] = "john.doe@example.com",
            ["IsActive"] = true
        };
        return Encoding.UTF8.GetBytes(obj.ToJsonString());
    }

    #endregion
}
