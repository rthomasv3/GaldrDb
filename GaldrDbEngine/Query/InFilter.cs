using System;
using System.Collections.Generic;

namespace GaldrDbCore.Query;

public sealed class InFilter<TDocument, TField> : IFieldFilter
{
    private readonly GaldrField<TDocument, TField> _field;
    private readonly HashSet<TField> _values;
    private readonly TField[] _valuesArray;

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
        get { return FieldOp.In; }
    }

    public IReadOnlyCollection<TField> Values
    {
        get { return _valuesArray; }
    }

    public InFilter(GaldrField<TDocument, TField> field, TField[] values)
    {
        _field = field;
        _valuesArray = values;
        _values = new HashSet<TField>(values);
    }

    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        bool result = _values.Contains(fieldValue);
        return result;
    }

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

    public byte[] GetIndexKeyEndBytes()
    {
        // In queries don't have an end key in the traditional sense
        // Each value is looked up individually
        return null;
    }

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
