using System;
using System.Collections.Generic;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where a field value is in a set of values using runtime field access.
/// </summary>
internal sealed class DynamicInFilter : IFieldFilter
{
    private readonly string _fieldName;
    private readonly GaldrFieldType _fieldType;
    private readonly HashSet<object> _valuesSet;
    private readonly object[] _valuesArray;
    private readonly bool _isIndexed;

    public string FieldName
    {
        get { return _fieldName; }
    }

    public GaldrFieldType FieldType
    {
        get { return _fieldType; }
    }

    public bool IsIndexed
    {
        get { return _isIndexed; }
    }

    public FieldOp Operation
    {
        get { return FieldOp.In; }
    }

    public Type DocumentType
    {
        get { return typeof(JsonDocument); }
    }

    public IReadOnlyCollection<object> Values
    {
        get { return _valuesArray; }
    }

    public DynamicInFilter(string fieldName, GaldrFieldType fieldType, object[] values, bool isIndexed)
    {
        _fieldName = fieldName;
        _fieldType = fieldType;
        _valuesArray = values;
        _valuesSet = new HashSet<object>(values);
        _isIndexed = isIndexed;
    }

    public bool Evaluate(object document)
    {
        return EvaluateDocument((JsonDocument)document);
    }

    public bool EvaluateDocument(JsonDocument doc)
    {
        bool result;

        if (!doc.TryGetValue(_fieldName, out System.Text.Json.Nodes.JsonNode node) || node == null)
        {
            result = _valuesSet.Contains(null);
        }
        else
        {
            object fieldValue = ExtractValue(node);
            result = _valuesSet.Contains(fieldValue);
        }

        return result;
    }

    private object ExtractValue(System.Text.Json.Nodes.JsonNode node)
    {
        return _fieldType switch
        {
            GaldrFieldType.Int32 => node.GetValue<int>(),
            GaldrFieldType.Int64 => node.GetValue<long>(),
            GaldrFieldType.String => node.GetValue<string>(),
            GaldrFieldType.Boolean => node.GetValue<bool>(),
            GaldrFieldType.DateTime => node.GetValue<DateTime>(),
            GaldrFieldType.DateTimeOffset => node.GetValue<DateTimeOffset>(),
            GaldrFieldType.Guid => node.GetValue<Guid>(),
            GaldrFieldType.Double => node.GetValue<double>(),
            GaldrFieldType.Decimal => node.GetValue<decimal>(),
            GaldrFieldType.Byte => node.GetValue<byte>(),
            GaldrFieldType.SByte => node.GetValue<sbyte>(),
            GaldrFieldType.Int16 => node.GetValue<short>(),
            GaldrFieldType.UInt16 => node.GetValue<ushort>(),
            GaldrFieldType.UInt32 => node.GetValue<uint>(),
            GaldrFieldType.UInt64 => node.GetValue<ulong>(),
            GaldrFieldType.Single => node.GetValue<float>(),
            GaldrFieldType.Char => node.GetValue<string>()[0],
            GaldrFieldType.TimeSpan => TimeSpan.Parse(node.GetValue<string>()),
            GaldrFieldType.DateOnly => DateOnly.Parse(node.GetValue<string>()),
            GaldrFieldType.TimeOnly => TimeOnly.Parse(node.GetValue<string>()),
            _ => node.GetValue<string>()
        };
    }

    public byte[] GetIndexKeyBytes()
    {
        byte[] result = null;

        if (_valuesArray.Length > 0)
        {
            result = IndexKeyEncoder.Encode(_valuesArray[0], _fieldType);
        }

        return result;
    }

    public byte[] GetIndexKeyEndBytes()
    {
        return null;
    }

    /// <summary>
    /// Gets the encoded index key bytes for all values in the set.
    /// </summary>
    /// <returns>Encoded key bytes for each value.</returns>
    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        List<byte[]> keys = new List<byte[]>(_valuesArray.Length);
        foreach (object value in _valuesArray)
        {
            keys.Add(IndexKeyEncoder.Encode(value, _fieldType));
        }
        return keys;
    }
}
