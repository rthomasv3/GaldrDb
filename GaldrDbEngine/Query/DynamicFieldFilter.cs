using System;
using System.Collections.Generic;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that compares a document field against a value using runtime field access.
/// </summary>
internal sealed class DynamicFieldFilter : IFieldFilter
{
    private readonly string _fieldName;
    private readonly GaldrFieldType _fieldType;
    private readonly FieldOp _op;
    private readonly object _value;
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
        get { return _op; }
    }

    public Type DocumentType
    {
        get { return typeof(JsonDocument); }
    }

    public object Value
    {
        get { return _value; }
    }

    public DynamicFieldFilter(string fieldName, GaldrFieldType fieldType, FieldOp op, object value, bool isIndexed)
    {
        ValidateOperation(fieldType, op);
        _fieldName = fieldName;
        _fieldType = fieldType;
        _op = op;
        _value = value;
        _isIndexed = isIndexed;
    }

    private static void ValidateOperation(GaldrFieldType fieldType, FieldOp op)
    {
        bool isStringField = fieldType == GaldrFieldType.String;
        bool isStringOperation = op == FieldOp.StartsWith || op == FieldOp.EndsWith || op == FieldOp.Contains;

        if (isStringOperation && !isStringField)
        {
            throw new ArgumentException($"Operation '{op}' is only supported for string fields.");
        }

        if (op == FieldOp.Between || op == FieldOp.In || op == FieldOp.NotIn)
        {
            throw new ArgumentException($"Operation '{op}' is not supported in Where(). Use WhereBetween(), WhereIn(), or WhereNotIn() instead.");
        }
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
            result = _op == FieldOp.Equals && _value == null;
        }
        else
        {
            object fieldValue = ExtractValue(node);
            result = EvaluateComparison(fieldValue, _value, _op);
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

    private static bool EvaluateComparison(object fieldValue, object filterValue, FieldOp op)
    {
        bool result;

        switch (op)
        {
            case FieldOp.Equals:
                result = Equals(fieldValue, filterValue);
                break;

            case FieldOp.NotEquals:
                result = !Equals(fieldValue, filterValue);
                break;

            case FieldOp.GreaterThan:
                result = fieldValue != null && CompareValues(fieldValue, filterValue) > 0;
                break;

            case FieldOp.GreaterThanOrEqual:
                result = fieldValue != null && CompareValues(fieldValue, filterValue) >= 0;
                break;

            case FieldOp.LessThan:
                result = fieldValue != null && CompareValues(fieldValue, filterValue) < 0;
                break;

            case FieldOp.LessThanOrEqual:
                result = fieldValue != null && CompareValues(fieldValue, filterValue) <= 0;
                break;

            case FieldOp.StartsWith:
                result = fieldValue is string s1 && filterValue is string v1 && s1.StartsWith(v1, StringComparison.Ordinal);
                break;

            case FieldOp.EndsWith:
                result = fieldValue is string s2 && filterValue is string v2 && s2.EndsWith(v2, StringComparison.Ordinal);
                break;

            case FieldOp.Contains:
                result = fieldValue is string s3 && filterValue is string v3 && s3.Contains(v3, StringComparison.Ordinal);
                break;

            default:
                throw new NotSupportedException($"Operation {op} not supported.");
        }

        return result;
    }

    private static int CompareValues(object a, object b)
    {
        int result = 0;

        if (a is IComparable comparable)
        {
            result = comparable.CompareTo(b);
        }

        return result;
    }

    public byte[] GetIndexKeyBytes()
    {
        return IndexKeyEncoder.Encode(_value, _fieldType);
    }

    public byte[] GetIndexKeyEndBytes()
    {
        byte[] result = null;

        if (_op == FieldOp.StartsWith && _value is string s)
        {
            result = IndexKeyEncoder.EncodePrefixEnd(s);
        }

        return result;
    }

    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        return null;
    }

    public object GetFilterValue()
    {
        return _value;
    }

    public object GetRangeMinValue()
    {
        return null;
    }

    public object GetRangeMaxValue()
    {
        return null;
    }

    public IReadOnlyList<int> GetInValuesAsInt32()
    {
        return null;
    }
}
