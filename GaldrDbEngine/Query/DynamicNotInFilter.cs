using System;
using System.Collections.Generic;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where a field value is not in a set of values using runtime field access.
/// </summary>
internal sealed class DynamicNotInFilter : IFieldFilter
{
    private readonly string _fieldName;
    private readonly GaldrFieldType _fieldType;
    private readonly HashSet<object> _valuesSet;
    private readonly object[] _valuesArray;

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
        get { return false; }
    }

    public FieldOp Operation
    {
        get { return FieldOp.NotIn; }
    }

    public Type DocumentType
    {
        get { return typeof(JsonDocument); }
    }

    public IReadOnlyCollection<object> Values
    {
        get { return _valuesArray; }
    }

    public DynamicNotInFilter(string fieldName, GaldrFieldType fieldType, object[] values)
    {
        _fieldName = fieldName;
        _fieldType = fieldType;
        _valuesArray = values;
        _valuesSet = new HashSet<object>(values);
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
            result = !_valuesSet.Contains(null);
        }
        else
        {
            object fieldValue = ExtractValue(node);
            result = !_valuesSet.Contains(fieldValue);
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
        return null;
    }

    public byte[] GetIndexKeyEndBytes()
    {
        return null;
    }

    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        return null;
    }
}
