using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where any element in a collection has a field value
/// within a range (inclusive on both ends).
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TElement">The collection element type.</typeparam>
/// <typeparam name="TField">The field type within elements.</typeparam>
public sealed class CollectionBetweenFilter<TDocument, TElement, TField> : IFieldFilter
    where TField : IComparable<TField>
{
    private readonly GaldrCollectionField<TDocument, TElement, TField> _field;
    private readonly TField _minValue;
    private readonly TField _maxValue;

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
        get { return FieldOp.Between; }
    }

    /// <inheritdoc/>
    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    /// <summary>The minimum value (inclusive).</summary>
    public TField MinValue
    {
        get { return _minValue; }
    }

    /// <summary>The maximum value (inclusive).</summary>
    public TField MaxValue
    {
        get { return _maxValue; }
    }

    /// <summary>
    /// Creates a new collection between filter.
    /// </summary>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    public CollectionBetweenFilter(GaldrCollectionField<TDocument, TElement, TField> field, TField minValue, TField maxValue)
    {
        _field = field;
        _minValue = minValue;
        _maxValue = maxValue;
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
        bool result = false;

        if (fieldValue != null)
        {
            int compareToMin = fieldValue.CompareTo(_minValue);
            int compareToMax = fieldValue.CompareTo(_maxValue);
            result = compareToMin >= 0 && compareToMax <= 0;
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
        return null;
    }

    /// <inheritdoc/>
    public object GetRangeMinValue()
    {
        return _minValue;
    }

    /// <inheritdoc/>
    public object GetRangeMaxValue()
    {
        return _maxValue;
    }

    /// <inheritdoc/>
    public IReadOnlyList<int> GetInValuesAsInt32()
    {
        return null;
    }
}
