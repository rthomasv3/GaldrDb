using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that compares a document field against a single value.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TField">The field type.</typeparam>
public sealed class FieldFilter<TDocument, TField> : IFieldFilter
{
    private readonly GaldrField<TDocument, TField> _field;
    private readonly FieldOp _op;
    private readonly TField _value;

    /// <inheritdoc/>
    public string FieldName
    {
        get { return _field.FieldName; }
    }

    /// <inheritdoc/>
    public GaldrFieldType FieldType
    {
        get { return _field.FieldType; }
    }

    /// <inheritdoc/>
    public bool IsIndexed
    {
        get { return _field.IsIndexed; }
    }

    /// <inheritdoc/>
    public FieldOp Operation
    {
        get { return _op; }
    }

    /// <inheritdoc/>
    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    /// <summary>The value to compare against.</summary>
    public TField Value
    {
        get { return _value; }
    }

    /// <summary>
    /// Creates a new field filter.
    /// </summary>
    /// <param name="field">The field to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <exception cref="ArgumentException">Thrown if the operation is invalid for the field type.</exception>
    public FieldFilter(GaldrField<TDocument, TField> field, FieldOp op, TField value)
    {
        ValidateOperation(field, op);
        _field = field;
        _op = op;
        _value = value;
    }

    private static void ValidateOperation(GaldrField<TDocument, TField> field, FieldOp op)
    {
        bool isStringField = field.FieldType == GaldrFieldType.String;
        bool isStringOperation = op == FieldOp.StartsWith || op == FieldOp.EndsWith || op == FieldOp.Contains;

        if (isStringOperation && !isStringField)
        {
            throw new ArgumentException($"Operation '{op}' is only supported for string fields, but field '{field.FieldName}' is of type '{field.FieldType}'.");
        }

        if (op == FieldOp.Between || op == FieldOp.In || op == FieldOp.NotIn)
        {
            throw new ArgumentException($"Operation '{op}' is not supported in Where(). Use WhereBetween(), WhereIn(), or WhereNotIn() instead.");
        }
    }

    /// <inheritdoc/>
    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        bool result;

        switch (_op)
        {
            case FieldOp.Equals:
                result = EqualityComparer<TField>.Default.Equals(fieldValue, _value);
                break;

            case FieldOp.NotEquals:
                result = !EqualityComparer<TField>.Default.Equals(fieldValue, _value);
                break;

            case FieldOp.GreaterThan:
                result = fieldValue != null && Comparer<TField>.Default.Compare(fieldValue, _value) > 0;
                break;

            case FieldOp.GreaterThanOrEqual:
                result = fieldValue != null && Comparer<TField>.Default.Compare(fieldValue, _value) >= 0;
                break;

            case FieldOp.LessThan:
                result = fieldValue != null && Comparer<TField>.Default.Compare(fieldValue, _value) < 0;
                break;

            case FieldOp.LessThanOrEqual:
                result = fieldValue != null && Comparer<TField>.Default.Compare(fieldValue, _value) <= 0;
                break;

            case FieldOp.StartsWith:
                result = EvaluateStartsWith(fieldValue);
                break;

            case FieldOp.EndsWith:
                result = EvaluateEndsWith(fieldValue);
                break;

            case FieldOp.Contains:
                result = EvaluateContains(fieldValue);
                break;

            default:
                throw new NotSupportedException($"Operation {_op} not supported for FieldFilter. Use specialized filter classes for Between/In operations.");
        }

        return result;
    }

    private bool EvaluateStartsWith(TField fieldValue)
    {
        bool result = false;
        if (fieldValue is string s && _value is string v)
        {
            result = s.StartsWith(v, StringComparison.Ordinal);
        }
        else if (fieldValue == null)
        {
            result = false;
        }
        else
        {
            throw new NotSupportedException($"StartsWith operation is only supported for string fields");
        }
        return result;
    }

    private bool EvaluateEndsWith(TField fieldValue)
    {
        bool result = false;
        if (fieldValue is string s && _value is string v)
        {
            result = s.EndsWith(v, StringComparison.Ordinal);
        }
        else if (fieldValue == null)
        {
            result = false;
        }
        else
        {
            throw new NotSupportedException($"EndsWith operation is only supported for string fields");
        }
        return result;
    }

    private bool EvaluateContains(TField fieldValue)
    {
        bool result = false;
        if (fieldValue is string s && _value is string v)
        {
            result = s.Contains(v, StringComparison.Ordinal);
        }
        else if (fieldValue == null)
        {
            result = false;
        }
        else
        {
            throw new NotSupportedException($"Contains operation is only supported for string fields");
        }
        return result;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyBytes()
    {
        byte[] result;
        if (_op == FieldOp.StartsWith && _value is string s)
        {
            result = IndexKeyEncoder.EncodePrefix(s);
        }
        else
        {
            result = IndexKeyEncoder.Encode(_value, _field.FieldType);
        }
        return result;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyEndBytes()
    {
        byte[] result = null;

        if (_op == FieldOp.StartsWith && _value is string s)
        {
            result = IndexKeyEncoder.EncodePrefixEnd(s);
        }

        return result;
    }

    /// <inheritdoc/>
    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        return null;
    }

    /// <inheritdoc/>
    public object GetFilterValue()
    {
        return _value;
    }

    /// <inheritdoc/>
    public object GetRangeMinValue()
    {
        return null;
    }

    /// <inheritdoc/>
    public object GetRangeMaxValue()
    {
        return null;
    }

    /// <inheritdoc/>
    public IReadOnlyList<int> GetInValuesAsInt32()
    {
        return null;
    }

    /// <inheritdoc/>
    public int GetCompoundEncodedSize()
    {
        return IndexKeyEncoder.GetCompoundEncodedSize(_value);
    }

    /// <inheritdoc/>
    public int EncodeCompoundFieldTo(byte[] buffer, int offset)
    {
        return IndexKeyEncoder.EncodeCompoundFieldTo(buffer, offset, _value);
    }

    /// <inheritdoc/>
    public int GetCompoundEncodedSizeMax()
    {
        return GetCompoundEncodedSize();
    }

    /// <inheritdoc/>
    public int EncodeCompoundFieldToMax(byte[] buffer, int offset)
    {
        return EncodeCompoundFieldTo(buffer, offset);
    }

    /// <inheritdoc/>
    public int GetCompoundEncodedSizeForPrefix()
    {
        int result;
        if (_op == FieldOp.StartsWith && _value is string s)
        {
            result = IndexKeyEncoder.GetCompoundPrefixSize(s);
        }
        else
        {
            result = GetCompoundEncodedSize();
        }
        return result;
    }

    /// <inheritdoc/>
    public int EncodeCompoundFieldToForPrefix(byte[] buffer, int offset)
    {
        int result;
        if (_op == FieldOp.StartsWith && _value is string s)
        {
            result = IndexKeyEncoder.EncodeCompoundPrefix(buffer, offset, s);
        }
        else
        {
            result = EncodeCompoundFieldTo(buffer, offset);
        }
        return result;
    }
}
