using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that compares elements in a collection field against a single value,
/// matching if any element satisfies the condition.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TElement">The collection element type.</typeparam>
/// <typeparam name="TField">The field type within elements.</typeparam>
public sealed class CollectionFieldFilter<TDocument, TElement, TField> : IFieldFilter
{
    private readonly GaldrCollectionField<TDocument, TElement, TField> _field;
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
        get { return false; }
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
    /// Creates a new collection field filter.
    /// </summary>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <exception cref="ArgumentException">Thrown if the operation is invalid for the field type.</exception>
    public CollectionFieldFilter(GaldrCollectionField<TDocument, TElement, TField> field, FieldOp op, TField value)
    {
        ValidateOperation(field, op);
        _field = field;
        _op = op;
        _value = value;
    }

    private static void ValidateOperation(GaldrCollectionField<TDocument, TElement, TField> field, FieldOp op)
    {
        bool isStringField = field.FieldType == GaldrFieldType.String;
        bool isStringOperation = op == FieldOp.StartsWith || op == FieldOp.EndsWith || op == FieldOp.Contains;

        if (isStringOperation && !isStringField)
        {
            throw new ArgumentException($"Operation '{op}' is only supported for string fields, but field '{field.FieldName}' is of type '{field.FieldType}'.");
        }

        if (op == FieldOp.Between || op == FieldOp.In || op == FieldOp.NotIn)
        {
            throw new ArgumentException($"Operation '{op}' is not supported in WhereAny(). Use WhereAnyBetween(), WhereAnyIn(), or WhereAnyNotIn() instead.");
        }
    }

    /// <inheritdoc/>
    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        IEnumerable<TElement> collection = _field.CollectionAccessor(doc);
        bool result = false;

        if (collection != null)
        {
            foreach (TElement element in collection)
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

    private bool EvaluateElement(TElement element)
    {
        TField fieldValue = _field.ElementAccessor(element);
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
                throw new NotSupportedException($"Operation {_op} not supported for CollectionFieldFilter.");
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
        return result;
    }

    private bool EvaluateEndsWith(TField fieldValue)
    {
        bool result = false;
        if (fieldValue is string s && _value is string v)
        {
            result = s.EndsWith(v, StringComparison.Ordinal);
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
        return result;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyBytes()
    {
        return null;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyEndBytes()
    {
        return null;
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
}
