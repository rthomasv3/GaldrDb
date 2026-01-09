using GaldrDbEngine.Storage;

namespace GaldrDbEngine.MVCC;

public sealed class CollectableVersion
{
    public string CollectionName { get; }
    public int DocumentId { get; }
    public DocumentVersion Previous { get; }
    public DocumentVersion ToRemove { get; }
    public DocumentLocation Location { get; }

    public CollectableVersion(
        string collectionName,
        int documentId,
        DocumentVersion previous,
        DocumentVersion toRemove,
        DocumentLocation location)
    {
        CollectionName = collectionName;
        DocumentId = documentId;
        Previous = previous;
        ToRemove = toRemove;
        Location = location;
    }
}
