using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class BetweenFilter<TDocument, TField> : IFieldFilter where TField : IComparable<TField>
{
    private readonly GaldrField<TDocument, TField> _field;
    private readonly TField _minValue;
    private readonly TField _maxValue;

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
        get { return FieldOp.Between; }
    }

    public Type DocumentType
    {
        get { return typeof(TDocument); }
    }

    public TField MinValue
    {
        get { return _minValue; }
    }

    public TField MaxValue
    {
        get { return _maxValue; }
    }

    public BetweenFilter(GaldrField<TDocument, TField> field, TField minValue, TField maxValue)
    {
        _field = field;
        _minValue = minValue;
        _maxValue = maxValue;
    }

    public bool Evaluate(object document)
    {
        TDocument doc = (TDocument)document;
        TField fieldValue = _field.Accessor(doc);

        // Between is inclusive on both ends
        int compareToMin = fieldValue.CompareTo(_minValue);
        int compareToMax = fieldValue.CompareTo(_maxValue);

        bool result = compareToMin >= 0 && compareToMax <= 0;
        return result;
    }

    public byte[] GetIndexKeyBytes()
    {
        // Returns the min value for range scan start
        return IndexKeyEncoder.Encode(_minValue, _field.FieldType);
    }

    public byte[] GetIndexKeyEndBytes()
    {
        // Returns the max value for range scan end
        return IndexKeyEncoder.Encode(_maxValue, _field.FieldType);
    }
}
