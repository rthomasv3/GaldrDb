using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where a field value is in a set of values.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TField">The field type.</typeparam>
public sealed class InFilter<TDocument, TField> : IFieldFilter
{
    private readonly GaldrField<TDocument, TField> _field;
    private readonly HashSet<TField> _values;
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
        get { return _field.IsIndexed; }
    }

    /// <inheritdoc/>
    public FieldOp Operation
    {
        get { return FieldOp.In; }
    }

    /// <inheritdoc/>
    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    /// <summary>The set of values to match against.</summary>
    public IReadOnlyCollection<TField> Values
    {
        get { return _valuesArray; }
    }

    /// <summary>
    /// Creates a new in filter.
    /// </summary>
    /// <param name="field">The field to filter on.</param>
    /// <param name="values">The set of values to match.</param>
    public InFilter(GaldrField<TDocument, TField> field, TField[] values)
    {
        _field = field;
        _valuesArray = values;
        _values = new HashSet<TField>(values);
    }

    /// <inheritdoc/>
    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        bool result = _values.Contains(fieldValue);
        return result;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyBytes()
    {
        // For In queries, we return the first value's key bytes
        // The query executor will handle multiple lookups
        if (_valuesArray.Length > 0)
        {
            return IndexKeyEncoder.Encode(_valuesArray[0], _field.FieldType);
        }
        return Array.Empty<byte>();
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyEndBytes()
    {
        // In queries don't have an end key in the traditional sense
        // Each value is looked up individually
        return null;
    }

    /// <summary>
    /// Gets the encoded index key bytes for all values in the set.
    /// </summary>
    /// <returns>Encoded key bytes for each value.</returns>
    public IEnumerable<byte[]> GetAllIndexKeyBytes()
    {
        List<byte[]> keys = new List<byte[]>(_valuesArray.Length);
        foreach (TField value in _valuesArray)
        {
            keys.Add(IndexKeyEncoder.Encode(value, _field.FieldType));
        }
        return keys;
    }
}
