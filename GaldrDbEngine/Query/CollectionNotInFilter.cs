using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where any element in a collection has a field value
/// not in the specified values.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TElement">The collection element type.</typeparam>
/// <typeparam name="TField">The field type within elements.</typeparam>
public sealed class CollectionNotInFilter<TDocument, TElement, TField> : IFieldFilter
{
    private readonly GaldrCollectionField<TDocument, TElement, TField> _field;
    private readonly HashSet<TField> _valuesSet;
    private readonly TField[] _valuesArray;

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
        get { return FieldOp.NotIn; }
    }

    /// <inheritdoc/>
    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    /// <summary>The values to exclude.</summary>
    public IReadOnlyCollection<TField> Values
    {
        get { return _valuesArray; }
    }

    /// <summary>
    /// Creates a new collection NOT IN filter.
    /// </summary>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="values">The values to exclude.</param>
    public CollectionNotInFilter(GaldrCollectionField<TDocument, TElement, TField> field, TField[] values)
    {
        _field = field;
        _valuesArray = values;
        _valuesSet = new HashSet<TField>(values);
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
        return !_valuesSet.Contains(fieldValue);
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
        int result;
        if (_valuesArray.Length > 0)
        {
            result = IndexKeyEncoder.GetCompoundEncodedSize(_valuesArray[0]);
        }
        else
        {
            result = IndexKeyEncoder.COMPOUND_NULL_SIZE;
        }
        return result;
    }

    /// <inheritdoc/>
    public int EncodeCompoundFieldTo(byte[] buffer, int offset)
    {
        int result;
        if (_valuesArray.Length > 0)
        {
            result = IndexKeyEncoder.EncodeCompoundFieldTo(buffer, offset, _valuesArray[0]);
        }
        else
        {
            result = IndexKeyEncoder.EncodeCompoundNull(buffer, offset);
        }
        return result;
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
        return GetCompoundEncodedSize();
    }

    /// <inheritdoc/>
    public int EncodeCompoundFieldToForPrefix(byte[] buffer, int offset)
    {
        return EncodeCompoundFieldTo(buffer, offset);
    }
}
