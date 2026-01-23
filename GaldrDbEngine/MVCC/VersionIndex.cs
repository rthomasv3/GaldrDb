using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

internal sealed class VersionIndex
{
    private readonly Dictionary<string, SortedDictionary<int, DocumentVersion>> _index;
    private readonly HashSet<DocumentKey> _gcCandidates;
    private readonly object _lock = new object();
    private int _multiVersionDocumentCount;

    public VersionIndex()
    {
        _index = new Dictionary<string, SortedDictionary<int, DocumentVersion>>();
        _gcCandidates = new HashSet<DocumentKey>();
        _multiVersionDocumentCount = 0;
    }

    public int MultiVersionDocumentCount
    {
        get
        {
            lock (_lock)
            {
                return _multiVersionDocumentCount;
            }
        }
    }

    public DocumentVersion GetLatestVersion(string collectionName, int documentId)
    {
        DocumentVersion result = null;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
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
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
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

    public List<int> GetDocumentIds(string collectionName)
    {
        List<int> result;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                result = new List<int>(collection.Keys);
            }
            else
            {
                result = new List<int>();
            }
        }

        return result;
    }

    public void AddVersion(string collectionName, int documentId, TxId createdBy, DocumentLocation location)
    {
        lock (_lock)
        {
            if (!_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                collection = new SortedDictionary<int, DocumentVersion>();
                _index[collectionName] = collection;
            }

            DocumentVersion previousVersion = null;
            if (collection.TryGetValue(documentId, out DocumentVersion existing))
            {
                previousVersion = existing;
                // Mark the previous version as superseded by this new version
                previousVersion.MarkDeleted(createdBy);

                // If this document was single-version, it now becomes multi-version
                if (previousVersion.PreviousVersion == null)
                {
                    _multiVersionDocumentCount++;
                }

                _gcCandidates.Add(new DocumentKey(collectionName, documentId));
            }

            DocumentVersion newVersion = new DocumentVersion(documentId, createdBy, location, previousVersion);
            collection[documentId] = newVersion;
        }
    }

    public void ValidateAndAddVersions(TxId createdBy, TxId snapshotTxId, IReadOnlyList<VersionOperation> operations)
    {
        lock (_lock)
        {
            // Phase 1: Validate all operations - if any conflict, throw before modifying anything
            foreach (VersionOperation op in operations)
            {
                if (_index.TryGetValue(op.CollectionName, out SortedDictionary<int, DocumentVersion> collection))
                {
                    if (collection.TryGetValue(op.DocumentId, out DocumentVersion existing))
                    {
                        bool hasConflict = false;

                        if (op.ReadVersionTxId.HasValue)
                        {
                            // For updates/deletes: check if the version we read is still current
                            // If someone else modified it after we read it, that's a conflict
                            hasConflict = existing.CreatedBy != op.ReadVersionTxId.Value;
                        }
                        else
                        {
                            // For inserts: check against snapshot (shouldn't happen for existing docs)
                            hasConflict = existing.CreatedBy > snapshotTxId;
                        }

                        if (hasConflict)
                        {
                            throw new WriteConflictException(
                                $"Document {op.CollectionName}/{op.DocumentId} was modified by transaction {existing.CreatedBy}",
                                op.CollectionName,
                                op.DocumentId,
                                existing.CreatedBy);
                        }
                    }
                }
            }

            // Phase 2: All validated - now apply all operations
            foreach (VersionOperation op in operations)
            {
                if (op.IsDelete)
                {
                    MarkDeletedInternal(op.CollectionName, op.DocumentId, createdBy);
                }
                else
                {
                    AddVersionInternal(op.CollectionName, op.DocumentId, createdBy, op.Location);
                }
            }
        }
    }

    private void AddVersionInternal(string collectionName, int documentId, TxId createdBy, DocumentLocation location)
    {
        if (!_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
        {
            collection = new SortedDictionary<int, DocumentVersion>();
            _index[collectionName] = collection;
        }

        DocumentVersion previousVersion = null;
        if (collection.TryGetValue(documentId, out DocumentVersion existing))
        {
            previousVersion = existing;
            previousVersion.MarkDeleted(createdBy);

            if (previousVersion.PreviousVersion == null)
            {
                _multiVersionDocumentCount++;
            }

            _gcCandidates.Add(new DocumentKey(collectionName, documentId));
        }

        DocumentVersion newVersion = new DocumentVersion(documentId, createdBy, location, previousVersion);
        collection[documentId] = newVersion;
    }

    private void MarkDeletedInternal(string collectionName, int documentId, TxId deletedBy)
    {
        if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
        {
            if (collection.TryGetValue(documentId, out DocumentVersion version))
            {
                version.MarkDeleted(deletedBy);
                _gcCandidates.Add(new DocumentKey(collectionName, documentId));
            }
        }
    }

    public void MarkDeleted(string collectionName, int documentId, TxId deletedBy)
    {
        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion version))
                {
                    version.MarkDeleted(deletedBy);
                    _gcCandidates.Add(new DocumentKey(collectionName, documentId));
                }
            }
        }
    }

    public bool HasVersion(string collectionName, int documentId)
    {
        bool result = false;

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
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
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
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
                _index[collectionName] = new SortedDictionary<int, DocumentVersion>();
            }
        }
    }

    public List<DocumentVersion> GetAllVisibleVersions(string collectionName, TxId snapshotTxId)
    {
        List<DocumentVersion> results = new List<DocumentVersion>();

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
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

    public List<DocumentVersion> GetVisibleVersionsForDocIds(string collectionName, IEnumerable<int> docIds, TxId snapshotTxId)
    {
        List<DocumentVersion> results = new List<DocumentVersion>();

        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                foreach (int docId in docIds)
                {
                    if (collection.TryGetValue(docId, out DocumentVersion currentVersion))
                    {
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
        }

        return results;
    }

    public void UnlinkVersion(string collectionName, int documentId, DocumentVersion previous, DocumentVersion toRemove)
    {
        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                DocumentKey docKey = new DocumentKey(collectionName, documentId);

                if (previous == null)
                {
                    // Removing the head version
                    if (toRemove.PreviousVersion == null)
                    {
                        // Last version in chain — remove document entry entirely
                        collection.Remove(documentId);
                        _gcCandidates.Remove(docKey);
                    }
                    else
                    {
                        // Promote next version to head
                        collection[documentId] = toRemove.PreviousVersion;
                        // Chain went from multi-version to still having versions, check if now single
                        if (toRemove.PreviousVersion.PreviousVersion == null)
                        {
                            _multiVersionDocumentCount--;
                            // If now single-version and not deleted, no longer a GC candidate
                            if (!toRemove.PreviousVersion.IsDeleted)
                            {
                                _gcCandidates.Remove(docKey);
                            }
                        }
                    }
                }
                else
                {
                    // Removing an interior version
                    previous.SetPreviousVersion(toRemove.PreviousVersion);

                    // If the chain now has only one version (head with no previous), decrement counter
                    if (collection.TryGetValue(documentId, out DocumentVersion head))
                    {
                        if (head.PreviousVersion == null)
                        {
                            _multiVersionDocumentCount--;
                            // If now single-version and not deleted, no longer a GC candidate
                            if (!head.IsDeleted)
                            {
                                _gcCandidates.Remove(docKey);
                            }
                        }
                    }
                }
            }
        }
    }

    internal List<string> GetCollectionNames()
    {
        lock (_lock)
        {
            return new List<string>(_index.Keys);
        }
    }

    internal int GetDocumentCount(string collectionName)
    {
        lock (_lock)
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                return collection.Count;
            }
            return 0;
        }
    }

    public void CollectGarbageVersions(TxId oldestSnapshot, List<CollectableVersion> results)
    {
        lock (_lock)
        {
            foreach (DocumentKey docKey in _gcCandidates)
            {
                if (!_index.TryGetValue(docKey.CollectionName, out SortedDictionary<int, DocumentVersion> collection))
                {
                    continue;
                }

                if (!collection.TryGetValue(docKey.DocId, out DocumentVersion head))
                {
                    continue;
                }

                // Handle single-version chains - check if deleted and collectable
                if (head.PreviousVersion == null)
                {
                    if (head.DeletedBy != TxId.MaxValue && head.DeletedBy <= oldestSnapshot)
                    {
                        CollectableVersion collectable = new CollectableVersion(
                            docKey.CollectionName,
                            docKey.DocId,
                            null,
                            head,
                            head.Location);
                        results.Add(collectable);
                    }
                    continue;
                }

                // Walk the chain and identify collectable versions
                DocumentVersion current = head;
                DocumentVersion previous = null;
                bool foundVisibleVersion = false;

                while (current != null)
                {
                    bool canCollect = false;

                    if (foundVisibleVersion)
                    {
                        // Version was superseded - collect if deleted at or before oldest snapshot
                        if (current.DeletedBy != TxId.MaxValue && current.DeletedBy <= oldestSnapshot)
                        {
                            canCollect = true;
                        }
                    }
                    else
                    {
                        if (current.IsVisibleTo(oldestSnapshot))
                        {
                            foundVisibleVersion = true;
                        }
                        else if (current.DeletedBy != TxId.MaxValue && current.DeletedBy <= oldestSnapshot)
                        {
                            // Version is not visible to oldest snapshot - safe to collect
                            canCollect = true;
                        }
                    }

                    if (canCollect)
                    {
                        CollectableVersion collectable = new CollectableVersion(
                            docKey.CollectionName,
                            docKey.DocId,
                            previous,
                            current,
                            current.Location);
                        results.Add(collectable);
                    }
                    else
                    {
                        previous = current;
                    }

                    current = current.PreviousVersion;
                }
            }
        }
    }
}
