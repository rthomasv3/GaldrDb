using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

public sealed class VersionIndex
{
    private readonly Dictionary<string, Dictionary<int, DocumentVersion>> _index;
    private readonly object _lock = new object();

    public VersionIndex()
    {
        _index = new Dictionary<string, Dictionary<int, DocumentVersion>>();
    }

    public DocumentVersion GetLatestVersion(string collectionName, int documentId)
    {
        DocumentVersion result = null;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion version))
                {
                    result = version;
                }
            }
        }

        return result;
    }

    public DocumentVersion GetVisibleVersion(string collectionName, int documentId, TxId snapshotTxId)
    {
        DocumentVersion result = null;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion currentVersion))
                {
                    while (currentVersion != null && result == null)
                    {
                        if (currentVersion.IsVisibleTo(snapshotTxId))
                        {
                            result = currentVersion;
                        }
                        else
                        {
                            currentVersion = currentVersion.PreviousVersion;
                        }
                    }
                }
            }
        }

        return result;
    }

    public void AddVersion(string collectionName, int documentId, TxId createdBy, DocumentLocation location)
    {
        lock (_lock)
        {
            if (!_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                collection = new Dictionary<int, DocumentVersion>();
                _index[collectionName] = collection;
            }

            DocumentVersion previousVersion = null;
            if (collection.TryGetValue(documentId, out DocumentVersion existing))
            {
                previousVersion = existing;
                // Mark the previous version as superseded by this new version
                previousVersion.MarkDeleted(createdBy);
            }

            DocumentVersion newVersion = new DocumentVersion(createdBy, location, previousVersion);
            collection[documentId] = newVersion;
        }
    }

    public void MarkDeleted(string collectionName, int documentId, TxId deletedBy)
    {
        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion version))
                {
                    version.MarkDeleted(deletedBy);
                }
            }
        }
    }

    public bool HasVersion(string collectionName, int documentId)
    {
        bool result = false;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                result = collection.ContainsKey(documentId);
            }
        }

        return result;
    }

    public int GetVersionCount(string collectionName, int documentId)
    {
        int count = 0;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion currentVersion))
                {
                    while (currentVersion != null)
                    {
                        count++;
                        currentVersion = currentVersion.PreviousVersion;
                    }
                }
            }
        }

        return count;
    }

    public void EnsureCollection(string collectionName)
    {
        lock (_lock)
        {
            if (!_index.ContainsKey(collectionName))
            {
                _index[collectionName] = new Dictionary<int, DocumentVersion>();
            }
        }
    }

    public List<DocumentVersion> GetAllVisibleVersions(string collectionName, TxId snapshotTxId)
    {
        List<DocumentVersion> results = new List<DocumentVersion>();

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out Dictionary<int, DocumentVersion> collection))
            {
                foreach (KeyValuePair<int, DocumentVersion> kvp in collection)
                {
                    DocumentVersion currentVersion = kvp.Value;

                    while (currentVersion != null)
                    {
                        if (currentVersion.IsVisibleTo(snapshotTxId))
                        {
                            results.Add(currentVersion);
                            break;
                        }

                        currentVersion = currentVersion.PreviousVersion;
                    }
                }
            }
        }

        return results;
    }

    public List<CollectionVersions> GetAllCollectionsForGC()
    {
        List<CollectionVersions> results = new List<CollectionVersions>();

        lock (_lock)
        {
            foreach (KeyValuePair<string, Dictionary<int, DocumentVersion>> collectionKvp in _index)
            {
                List<DocumentVersionChain> chains = new List<DocumentVersionChain>();

                foreach (KeyValuePair<int, DocumentVersion> docKvp in collectionKvp.Value)
                {
                    chains.Add(new DocumentVersionChain(collectionKvp.Key, docKvp.Key, docKvp.Value));
                }

                results.Add(new CollectionVersions(collectionKvp.Key, chains));
            }
        }

        return results;
    }

    public void UnlinkVersion(string collectionName, int documentId, DocumentVersion previous, DocumentVersion toRemove)
    {
        lock (_lock)
        {
            // Update the previous version's pointer to skip over the removed version
            previous.SetPreviousVersion(toRemove.PreviousVersion);
        }
    }
}
