using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class NotInFilter<TDocument, TField> : IFieldFilter
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
        get { return false; }
    }

    public FieldOp Operation
    {
        get { return FieldOp.NotIn; }
    }

    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    public IReadOnlyCollection<TField> Values
    {
        get { return _valuesArray; }
    }

    public NotInFilter(GaldrField<TDocument, TField> field, TField[] values)
    {
        _field = field;
        _valuesArray = values;
        _values = new HashSet<TField>(values);
    }

    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        bool result = !_values.Contains(fieldValue);
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
}
