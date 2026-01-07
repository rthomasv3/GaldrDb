namespace GaldrDbEngine.Query;

public interface IFieldFilter
{
    string FieldName { get; }
    GaldrFieldType FieldType { get; }
    bool IsIndexed { get; }
    FieldOp Operation { get; }

    bool Evaluate(object document);
    byte[] GetIndexKeyBytes();
    byte[] GetIndexKeyEndBytes();
}
