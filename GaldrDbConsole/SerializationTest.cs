using System;
using System.Text;
using System.Text.Json;
using GaldrDbConsole.Models;
using GaldrJson;

namespace GaldrDbConsole;

public static class SerializationTest
{
    private static readonly GaldrJsonSerializer _jsonSerializer = new GaldrJsonSerializer();
    private static readonly GaldrJsonOptions _jsonOptions = new GaldrJsonOptions()
    {
        PropertyNameCaseInsensitive = false,
        PropertyNamingPolicy = PropertyNamingPolicy.Exact,
        WriteIndented = false,
        DetectCycles = true,
    };

    public static void Run()
    {
        BenchmarkPerson person = new BenchmarkPerson
        {
            Id = 1,
            Name = "Test Person",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        };

        // Warm up
        for (int i = 0; i < 100; i++)
        {
            SerializeOldWay(person);
            SerializeNewWay(person);
        }

        // Force GC to get clean baseline
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long beforeOld = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            byte[] result = SerializeOldWay(person);
        }
        long afterOld = GC.GetAllocatedBytesForCurrentThread();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long beforeNew = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            byte[] result = SerializeNewWay(person);
        }
        long afterNew = GC.GetAllocatedBytesForCurrentThread();

        Console.WriteLine($"Old way (Serialize + GetBytes): {(afterOld - beforeOld) / 1000.0:F2} bytes/call");
        Console.WriteLine($"New way (SerializeTo + ToArray): {(afterNew - beforeNew) / 1000.0:F2} bytes/call");
        Console.WriteLine($"Difference: {((afterNew - beforeNew) - (afterOld - beforeOld)) / 1000.0:F2} bytes/call");

        // Also check what the actual JSON size is
        string json = _jsonSerializer.Serialize(person, _jsonOptions);
        Console.WriteLine($"\nJSON size: {Encoding.UTF8.GetByteCount(json)} bytes");
        Console.WriteLine($"String size (UTF-16): {json.Length * 2} bytes");

        // Check buffer state
        Utf8JsonWriter writer = Utf8JsonWriterCache.RentWriter(false, out GaldrBufferWriter buffer);
        Console.WriteLine($"\nBuffer capacity after rent: {buffer.Capacity} bytes");
        _jsonSerializer.SerializeTo(writer, person, _jsonOptions);
        Console.WriteLine($"Buffer written count: {buffer.WrittenCount} bytes");
        Console.WriteLine($"Buffer capacity after write: {buffer.Capacity} bytes");
    }

    private static byte[] SerializeOldWay(BenchmarkPerson person)
    {
        string json = _jsonSerializer.Serialize(person, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
        return jsonBytes;
    }

    private static byte[] SerializeNewWay(BenchmarkPerson person)
    {
        Utf8JsonWriter writer = Utf8JsonWriterCache.RentWriter(false, out GaldrBufferWriter buffer);
        _jsonSerializer.SerializeTo(writer, person, _jsonOptions);
        byte[] jsonBytes = buffer.WrittenSpan.ToArray();
        return jsonBytes;
    }
}
