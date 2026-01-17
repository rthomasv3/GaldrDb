using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// A filter that matches documents where a field value is not in a set of values.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TField">The field type.</typeparam>
public sealed class NotInFilter<TDocument, TField> : IFieldFilter
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

    /// <summary>The set of values to exclude.</summary>
    public IReadOnlyCollection<TField> Values
    {
        get { return _valuesArray; }
    }

    /// <summary>
    /// Creates a new not-in filter.
    /// </summary>
    /// <param name="field">The field to filter on.</param>
    /// <param name="values">The set of values to exclude.</param>
    public NotInFilter(GaldrField<TDocument, TField> field, TField[] values)
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

        bool result = !_values.Contains(fieldValue);
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
}
