namespace GaldrDbEngine.MVCC;

public sealed class DocumentVersionChain
{
    public string CollectionName { get; }
    public int DocumentId { get; }
    public DocumentVersion Head { get; }

    public DocumentVersionChain(string collectionName, int documentId, DocumentVersion head)
    {
        CollectionName = collectionName;
        DocumentId = documentId;
        Head = head;
    }
}
