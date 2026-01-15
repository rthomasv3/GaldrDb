namespace GaldrDbEngine.Schema;

public class OrphanedIndexInfo
{
    public string CollectionName { get; }
    public string FieldName { get; }

    public OrphanedIndexInfo(string collectionName, string fieldName)
    {
        CollectionName = collectionName;
        FieldName = fieldName;
    }
}
