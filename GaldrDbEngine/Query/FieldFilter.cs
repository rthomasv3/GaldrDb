using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class FieldFilter<TDocument, TField> : IFieldFilter
{
    private readonly GaldrField<TDocument, TField> _field;
    private readonly FieldOp _op;
    private readonly TField _value;

    public string FieldName
    {
        get { return _field.FieldName; }
    }

    public GaldrFieldType FieldType
    {
        get { return _field.FieldType; }
    }

    public bool IsIndexed
    {
        get { return _field.IsIndexed; }
    }

    public FieldOp Operation
    {
        get { return _op; }
    }

    public TField Value
    {
        get { return _value; }
    }

    public FieldFilter(GaldrField<TDocument, TField> field, FieldOp op, TField value)
    {
        _field = field;
        _op = op;
        _value = value;
    }

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
                result = Comparer<TField>.Default.Compare(fieldValue, _value) > 0;
                break;

            case FieldOp.GreaterThanOrEqual:
                result = Comparer<TField>.Default.Compare(fieldValue, _value) >= 0;
                break;

            case FieldOp.LessThan:
                result = Comparer<TField>.Default.Compare(fieldValue, _value) < 0;
                break;

            case FieldOp.LessThanOrEqual:
                result = Comparer<TField>.Default.Compare(fieldValue, _value) <= 0;
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
        if (fieldValue is string s && _value is string v)
        {
            return s.StartsWith(v, StringComparison.Ordinal);
        }
        throw new NotSupportedException($"StartsWith operation is only supported for string fields");
    }

    private bool EvaluateEndsWith(TField fieldValue)
    {
        if (fieldValue is string s && _value is string v)
        {
            return s.EndsWith(v, StringComparison.Ordinal);
        }
        throw new NotSupportedException($"EndsWith operation is only supported for string fields");
    }

    private bool EvaluateContains(TField fieldValue)
    {
        if (fieldValue is string s && _value is string v)
        {
            return s.Contains(v, StringComparison.Ordinal);
        }
        throw new NotSupportedException($"Contains operation is only supported for string fields");
    }

    public byte[] GetIndexKeyBytes()
    {
        return IndexKeyEncoder.Encode(_value, _field.FieldType);
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
}
