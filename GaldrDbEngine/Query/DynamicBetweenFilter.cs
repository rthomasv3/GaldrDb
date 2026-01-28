using System;
using System.Collections.Generic;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches values within a range (inclusive on both ends) using runtime field access.
/// </summary>
internal sealed class DynamicBetweenFilter : IFieldFilter
{
    private readonly string _fieldName;
    private readonly GaldrFieldType _fieldType;
    private readonly object _minValue;
    private readonly object _maxValue;
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
        get { return FieldOp.Between; }
    }

    public Type DocumentType
    {
        get { return typeof(JsonDocument); }
    }

    public object MinValue
    {
        get { return _minValue; }
    }

    public object MaxValue
    {
        get { return _maxValue; }
    }

    public DynamicBetweenFilter(string fieldName, GaldrFieldType fieldType, object minValue, object maxValue, bool isIndexed)
    {
        _fieldName = fieldName;
        _fieldType = fieldType;
        _minValue = minValue;
        _maxValue = maxValue;
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
            result = false;
        }
        else
        {
            object fieldValue = ExtractValue(node);

            if (fieldValue is IComparable comparable)
            {
                int compareToMin = comparable.CompareTo(_minValue);
                int compareToMax = comparable.CompareTo(_maxValue);
                result = compareToMin >= 0 && compareToMax <= 0;
            }
            else
            {
                result = false;
            }
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
        return IndexKeyEncoder.Encode(_minValue, _fieldType);
    }

    public byte[] GetIndexKeyEndBytes()
    {
        return IndexKeyEncoder.Encode(_maxValue, _fieldType);
    }

    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        return null;
    }

    public object GetFilterValue()
    {
        return null;
    }

    public object GetRangeMinValue()
    {
        return _minValue;
    }

    public object GetRangeMaxValue()
    {
        return _maxValue;
    }
}
