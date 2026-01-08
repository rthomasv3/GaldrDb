namespace GaldrDbEngine.Transactions;

public class IndexFieldEntry
{
    public string FieldName { get; }
    public byte[] KeyBytes { get; }

    public IndexFieldEntry(string fieldName, byte[] keyBytes)
    {
        FieldName = fieldName;
        KeyBytes = keyBytes;
    }
}
