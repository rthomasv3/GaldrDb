using System;

namespace GaldrDbEngine.Query;

public sealed class GaldrField<TDocument, TField>
{
    public string FieldName { get; }
    public GaldrFieldType FieldType { get; }
    public bool IsIndexed { get; }
    public Func<TDocument, TField> Accessor { get; }

    public GaldrField(
        string fieldName,
        GaldrFieldType fieldType,
        bool isIndexed,
        Func<TDocument, TField> accessor)
    {
        FieldName = fieldName;
        FieldType = fieldType;
        IsIndexed = isIndexed;
        Accessor = accessor;
    }
}
