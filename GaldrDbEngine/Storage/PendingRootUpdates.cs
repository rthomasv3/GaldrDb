using System.Collections.Generic;

namespace GaldrDbEngine.Storage;

internal class PendingRootUpdates
{
    // Collection name -> new root page ID
    public Dictionary<string, int> CollectionRoots { get; } = new Dictionary<string, int>();

    // "collectionName:indexName" -> new root page ID
    public Dictionary<string, int> IndexRoots { get; } = new Dictionary<string, int>();

    public void SetCollectionRoot(string collectionName, int rootPageId)
    {
        CollectionRoots[collectionName] = rootPageId;
    }

    public void SetIndexRoot(string collectionName, string indexName, int rootPageId)
    {
        string key = $"{collectionName}:{indexName}";
        IndexRoots[key] = rootPageId;
    }

    public bool TryGetCollectionRoot(string collectionName, out int rootPageId)
    {
        return CollectionRoots.TryGetValue(collectionName, out rootPageId);
    }

    public bool TryGetIndexRoot(string collectionName, string indexName, out int rootPageId)
    {
        string key = $"{collectionName}:{indexName}";
        return IndexRoots.TryGetValue(key, out rootPageId);
    }

    public void Clear()
    {
        CollectionRoots.Clear();
        IndexRoots.Clear();
    }
}
