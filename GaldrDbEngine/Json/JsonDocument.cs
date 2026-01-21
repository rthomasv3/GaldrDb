using System;
using System.Text.Json;
using System.Text.Json.Nodes;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Json;

/// <summary>
/// Thin wrapper around System.Text.Json.Nodes.JsonObject providing typed accessors
/// and optimized serialization with pooled buffers.
/// </summary>
public sealed class JsonDocument
{
    private static readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions
    {
        WriteIndented = false
    };

    private readonly JsonObject _node;

    /// <summary>
    /// Creates an empty JSON document.
    /// </summary>
    public JsonDocument()
    {
        _node = new JsonObject();
    }

    /// <summary>
    /// Creates a wrapper around an existing JsonObject.
    /// </summary>
    public JsonDocument(JsonObject node)
    {
        _node = node ?? throw new ArgumentNullException(nameof(node));
    }

    /// <summary>
    /// Gets the underlying JsonObject.
    /// </summary>
    public JsonObject Node
    {
        get { return _node; }
    }

    /// <summary>
    /// Gets the number of fields in this document.
    /// </summary>
    public int FieldCount
    {
        get { return _node.Count; }
    }

    #region Parsing

    /// <summary>
    /// Parses a JSON document from a UTF-8 encoded byte array.
    /// </summary>
    public static JsonDocument Parse(byte[] utf8Json)
    {
        JsonNode node = JsonNode.Parse(utf8Json);
        return new JsonDocument(node.AsObject());
    }

    /// <summary>
    /// Parses a JSON document from a UTF-8 encoded byte span.
    /// </summary>
    public static JsonDocument Parse(ReadOnlySpan<byte> utf8Json)
    {
        JsonNode node = JsonNode.Parse(utf8Json);
        return new JsonDocument(node.AsObject());
    }

    /// <summary>
    /// Parses a JSON document from a string.
    /// </summary>
    public static JsonDocument Parse(string json)
    {
        JsonNode node = JsonNode.Parse(json);
        return new JsonDocument(node.AsObject());
    }

    #endregion

    #region Field Access

    /// <summary>
    /// Checks if a field exists.
    /// </summary>
    public bool HasField(string fieldName)
    {
        return _node.ContainsKey(fieldName);
    }

    /// <summary>
    /// Tries to get a field's JsonNode.
    /// </summary>
    public bool TryGetValue(string fieldName, out JsonNode value)
    {
        return _node.TryGetPropertyValue(fieldName, out value);
    }

    /// <summary>
    /// Gets a field as an int.
    /// </summary>
    public int GetInt32(string fieldName)
    {
        return _node[fieldName].GetValue<int>();
    }

    /// <summary>
    /// Gets a field as a long.
    /// </summary>
    public long GetInt64(string fieldName)
    {
        return _node[fieldName].GetValue<long>();
    }

    /// <summary>
    /// Gets a field as a double.
    /// </summary>
    public double GetDouble(string fieldName)
    {
        return _node[fieldName].GetValue<double>();
    }

    /// <summary>
    /// Gets a field as a decimal.
    /// </summary>
    public decimal GetDecimal(string fieldName)
    {
        return _node[fieldName].GetValue<decimal>();
    }

    /// <summary>
    /// Gets a field as a boolean.
    /// </summary>
    public bool GetBoolean(string fieldName)
    {
        return _node[fieldName].GetValue<bool>();
    }

    /// <summary>
    /// Gets a field as a string.
    /// </summary>
    public string GetString(string fieldName)
    {
        return _node[fieldName].GetValue<string>();
    }

    /// <summary>
    /// Gets a field as a DateTime.
    /// </summary>
    public DateTime GetDateTime(string fieldName)
    {
        return _node[fieldName].GetValue<DateTime>();
    }

    /// <summary>
    /// Gets a field as a DateTimeOffset.
    /// </summary>
    public DateTimeOffset GetDateTimeOffset(string fieldName)
    {
        return _node[fieldName].GetValue<DateTimeOffset>();
    }

    /// <summary>
    /// Gets a field as a Guid.
    /// </summary>
    public Guid GetGuid(string fieldName)
    {
        return _node[fieldName].GetValue<Guid>();
    }

    /// <summary>
    /// Gets a field as a nested JsonDocument.
    /// </summary>
    public JsonDocument GetObject(string fieldName)
    {
        return new JsonDocument(_node[fieldName].AsObject());
    }

    /// <summary>
    /// Gets a field as an array.
    /// </summary>
    public JsonArray GetArray(string fieldName)
    {
        return _node[fieldName].AsArray();
    }

    #endregion

    #region Mutation

    /// <summary>
    /// Sets a field to an int value.
    /// </summary>
    public void SetInt32(string fieldName, int value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a long value.
    /// </summary>
    public void SetInt64(string fieldName, long value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a double value.
    /// </summary>
    public void SetDouble(string fieldName, double value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a decimal value.
    /// </summary>
    public void SetDecimal(string fieldName, decimal value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a boolean value.
    /// </summary>
    public void SetBoolean(string fieldName, bool value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a string value.
    /// </summary>
    public void SetString(string fieldName, string value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a DateTime value.
    /// </summary>
    public void SetDateTime(string fieldName, DateTime value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a DateTimeOffset value.
    /// </summary>
    public void SetDateTimeOffset(string fieldName, DateTimeOffset value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to a Guid value.
    /// </summary>
    public void SetGuid(string fieldName, Guid value)
    {
        _node[fieldName] = value.ToString("D");
    }

    /// <summary>
    /// Sets a field to a nested object.
    /// </summary>
    public void SetObject(string fieldName, JsonDocument value)
    {
        _node[fieldName] = value.Node;
    }

    /// <summary>
    /// Sets a field to an array.
    /// </summary>
    public void SetArray(string fieldName, JsonArray value)
    {
        _node[fieldName] = value;
    }

    /// <summary>
    /// Sets a field to null.
    /// </summary>
    public void SetNull(string fieldName)
    {
        _node[fieldName] = null;
    }

    /// <summary>
    /// Removes a field from the document.
    /// </summary>
    public bool Remove(string fieldName)
    {
        return _node.Remove(fieldName);
    }

    #endregion

    #region Serialization

    /// <summary>
    /// Serializes this document to a UTF-8 encoded byte array using a pooled writer.
    /// </summary>
    public byte[] ToUtf8Bytes()
    {
        PooledJsonWriter pooledWriter = JsonWriterPool.Rent();
        try
        {
            _node.WriteTo(pooledWriter.Writer);
            pooledWriter.Writer.Flush();
            return pooledWriter.WrittenSpan.ToArray();
        }
        finally
        {
            JsonWriterPool.Return(pooledWriter);
        }
    }

    /// <summary>
    /// Serializes this document to a JSON string.
    /// </summary>
    public string ToJsonString()
    {
        return _node.ToJsonString(_serializerOptions);
    }

    /// <summary>
    /// Writes this document to a Utf8JsonWriter.
    /// </summary>
    public void WriteTo(Utf8JsonWriter writer)
    {
        _node.WriteTo(writer);
    }

    #endregion
}
