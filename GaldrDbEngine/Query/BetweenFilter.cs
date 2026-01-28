using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches values within a range (inclusive on both ends).
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TField">The field type.</typeparam>
public sealed class BetweenFilter<TDocument, TField> : IFieldFilter where TField : IComparable<TField>
{
    private readonly GaldrField<TDocument, TField> _field;
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
        get { return _field.IsIndexed; }
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

    /// <summary>The minimum value of the range (inclusive).</summary>
    public TField MinValue
    {
        get { return _minValue; }
    }

    /// <summary>The maximum value of the range (inclusive).</summary>
    public TField MaxValue
    {
        get { return _maxValue; }
    }

    /// <summary>
    /// Creates a new between filter.
    /// </summary>
    /// <param name="field">The field to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    public BetweenFilter(GaldrField<TDocument, TField> field, TField minValue, TField maxValue)
    {
        _field = field;
        _minValue = minValue;
        _maxValue = maxValue;
    }

    /// <inheritdoc/>
    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        // Between is inclusive on both ends
        int compareToMin = fieldValue.CompareTo(_minValue);
        int compareToMax = fieldValue.CompareTo(_maxValue);

        return compareToMin >= 0 && compareToMax <= 0;
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyBytes()
    {
        // Returns the min value for range scan start
        return IndexKeyEncoder.Encode(_minValue, _field.FieldType);
    }

    /// <inheritdoc/>
    public byte[] GetIndexKeyEndBytes()
    {
        // Returns the max value for range scan end
        return IndexKeyEncoder.Encode(_maxValue, _field.FieldType);
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
}
