using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where any element in a collection field satisfies the condition,
/// using runtime field access on JSON documents.
/// </summary>
internal sealed class DynamicCollectionFieldFilter : IFieldFilter
{
    private readonly string _fieldName;
    private readonly string _collectionPath;
    private readonly string _elementFieldName;
    private readonly GaldrFieldType _fieldType;
    private readonly FieldOp _op;
    private readonly object _value;

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

    public DynamicCollectionFieldFilter(string fieldName, GaldrFieldType fieldType, FieldOp op, object value)
    {
        ValidateOperation(fieldType, op);
        _fieldName = fieldName;
        _fieldType = fieldType;
        _op = op;
        _value = value;

        int lastDotIndex = fieldName.LastIndexOf('.');
        if (lastDotIndex > 0)
        {
            _collectionPath = fieldName.Substring(0, lastDotIndex);
            _elementFieldName = fieldName.Substring(lastDotIndex + 1);
        }
        else
        {
            _collectionPath = fieldName;
            _elementFieldName = null;
        }
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
            throw new ArgumentException($"Operation '{op}' is not supported in WhereAny(). Use WhereAnyBetween(), WhereAnyIn(), or WhereAnyNotIn() instead.");
        }
    }

    public bool Evaluate(object document)
    {
        return EvaluateDocument((JsonDocument)document);
    }

    public bool EvaluateDocument(JsonDocument doc)
    {
        bool result = false;
        JsonNode collectionNode;
        bool hasCollection = doc.TryGetNestedValue(_collectionPath, out collectionNode);

        if (hasCollection && collectionNode is JsonArray array)
        {
            foreach (JsonNode element in array)
            {
                if (element != null && EvaluateElement(element))
                {
                    result = true;
                    break;
                }
            }
        }

        return result;
    }

    private bool EvaluateElement(JsonNode element)
    {
        bool result = false;
        JsonNode valueNode = null;

        if (_elementFieldName != null && element is JsonObject obj)
        {
            obj.TryGetPropertyValue(_elementFieldName, out valueNode);
        }
        else
        {
            valueNode = element;
        }

        if (valueNode != null)
        {
            object fieldValue = ExtractValue(valueNode);
            result = EvaluateComparison(fieldValue, _value, _op);
        }

        return result;
    }

    private object ExtractValue(JsonNode node)
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

    public int GetCompoundEncodedSize()
    {
        return IndexKeyEncoder.GetCompoundEncodedSizeBoxed(_value, _fieldType);
    }

    public int EncodeCompoundFieldTo(byte[] buffer, int offset)
    {
        return IndexKeyEncoder.EncodeCompoundFieldToBoxed(buffer, offset, _value, _fieldType);
    }

    public int GetCompoundEncodedSizeMax()
    {
        return GetCompoundEncodedSize();
    }

    public int EncodeCompoundFieldToMax(byte[] buffer, int offset)
    {
        return EncodeCompoundFieldTo(buffer, offset);
    }

    public int GetCompoundEncodedSizeForPrefix()
    {
        return GetCompoundEncodedSize();
    }

    public int EncodeCompoundFieldToForPrefix(byte[] buffer, int offset)
    {
        return EncodeCompoundFieldTo(buffer, offset);
    }
}
