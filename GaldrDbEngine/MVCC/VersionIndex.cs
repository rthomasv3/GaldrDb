using System.Collections.Generic;
using System.Threading;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

internal sealed class VersionIndex
{
    private readonly Dictionary<string, SortedDictionary<int, DocumentVersion>> _index;
    private readonly HashSet<DocumentKey> _gcCandidates;
    private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
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
            _lock.EnterReadLock();
            try
            {
                return _multiVersionDocumentCount;
            }
            finally
            {
                _lock.ExitReadLock();
            }
        }
    }

    public DocumentVersion GetLatestVersion(string collectionName, int documentId)
    {
        DocumentVersion result = null;

        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion version))
                {
                    result = version;
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    public DocumentVersion GetVisibleVersion(string collectionName, int documentId, ulong snapshotCSN)
    {
        DocumentVersion result = null;

        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion currentVersion))
                {
                    while (currentVersion != null && result == null)
                    {
                        if (currentVersion.IsVisibleTo(snapshotCSN))
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
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    public List<int> GetDocumentIds(string collectionName)
    {
        List<int> result;

        _lock.EnterReadLock();
        try
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
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    public void AddVersion(string collectionName, int documentId, TxId createdBy, ulong commitCSN, DocumentLocation location)
    {
        _lock.EnterWriteLock();
        try
        {
            AddVersionInternal(collectionName, documentId, createdBy, commitCSN, location);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void ValidateVersions(TxId createdBy, TxId snapshotTxId, IReadOnlyList<VersionOperation> operations)
    {
        _lock.EnterReadLock();
        try
        {
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
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void AddVersions(TxId createdBy, ulong commitCSN, IReadOnlyList<VersionOperation> operations)
    {
        _lock.EnterWriteLock();
        try
        {
            foreach (VersionOperation op in operations)
            {
                if (op.IsDelete)
                {
                    MarkDeletedInternal(op.CollectionName, op.DocumentId, commitCSN);
                }
                else
                {
                    AddVersionInternal(op.CollectionName, op.DocumentId, createdBy, commitCSN, op.Location);
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private void AddVersionInternal(string collectionName, int documentId, TxId createdBy, ulong commitCSN, DocumentLocation location)
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
            previousVersion.MarkDeleted(commitCSN);

            if (previousVersion.PreviousVersion == null)
            {
                _multiVersionDocumentCount++;
            }

            _gcCandidates.Add(new DocumentKey(collectionName, documentId));
        }

        DocumentVersion newVersion = new DocumentVersion(documentId, createdBy, commitCSN, location, previousVersion);
        collection[documentId] = newVersion;
    }

    private void MarkDeletedInternal(string collectionName, int documentId, ulong deletedCSN)
    {
        if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
        {
            if (collection.TryGetValue(documentId, out DocumentVersion version))
            {
                version.MarkDeleted(deletedCSN);
                _gcCandidates.Add(new DocumentKey(collectionName, documentId));
            }
        }
    }

    public void MarkDeleted(string collectionName, int documentId, ulong deletedCSN)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                if (collection.TryGetValue(documentId, out DocumentVersion version))
                {
                    version.MarkDeleted(deletedCSN);
                    _gcCandidates.Add(new DocumentKey(collectionName, documentId));
                }
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public bool HasVersion(string collectionName, int documentId)
    {
        bool result = false;

        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                result = collection.ContainsKey(documentId);
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return result;
    }

    public int GetVersionCount(string collectionName, int documentId)
    {
        int count = 0;

        _lock.EnterReadLock();
        try
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
        finally
        {
            _lock.ExitReadLock();
        }

        return count;
    }

    public void EnsureCollection(string collectionName)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_index.ContainsKey(collectionName))
            {
                _index[collectionName] = new SortedDictionary<int, DocumentVersion>();
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public List<DocumentVersion> GetAllVisibleVersions(string collectionName, ulong snapshotCSN)
    {
        List<DocumentVersion> results = new List<DocumentVersion>();

        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                foreach (KeyValuePair<int, DocumentVersion> kvp in collection)
                {
                    DocumentVersion currentVersion = kvp.Value;

                    while (currentVersion != null)
                    {
                        if (currentVersion.IsVisibleTo(snapshotCSN))
                        {
                            results.Add(currentVersion);
                            break;
                        }

                        currentVersion = currentVersion.PreviousVersion;
                    }
                }
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }

        return results;
    }

    public List<DocumentVersion> GetVisibleVersionsForDocIds(string collectionName, IEnumerable<int> docIds, ulong snapshotCSN)
    {
        List<DocumentVersion> results = new List<DocumentVersion>();

        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                foreach (int docId in docIds)
                {
                    if (collection.TryGetValue(docId, out DocumentVersion currentVersion))
                    {
                        while (currentVersion != null)
                        {
                            if (currentVersion.IsVisibleTo(snapshotCSN))
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
        finally
        {
            _lock.ExitReadLock();
        }

        return results;
    }

    public void UnlinkVersion(string collectionName, int documentId, DocumentVersion previous, DocumentVersion toRemove)
    {
        _lock.EnterWriteLock();
        try
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
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    internal List<string> GetCollectionNames()
    {
        _lock.EnterReadLock();
        try
        {
            return new List<string>(_index.Keys);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    internal int GetDocumentCount(string collectionName)
    {
        _lock.EnterReadLock();
        try
        {
            if (_index.TryGetValue(collectionName, out SortedDictionary<int, DocumentVersion> collection))
            {
                return collection.Count;
            }
            return 0;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void CollectGarbageVersions(ulong oldestSnapshotCSN, List<CollectableVersion> results)
    {
        _lock.EnterReadLock();
        try
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
                    if (head.DeletedCSN != ulong.MaxValue && head.DeletedCSN <= oldestSnapshotCSN)
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
                        if (current.DeletedCSN != ulong.MaxValue && current.DeletedCSN <= oldestSnapshotCSN)
                        {
                            canCollect = true;
                        }
                    }
                    else
                    {
                        if (current.IsVisibleTo(oldestSnapshotCSN))
                        {
                            foundVisibleVersion = true;
                        }
                        else if (current.DeletedCSN != ulong.MaxValue && current.DeletedCSN <= oldestSnapshotCSN)
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
        finally
        {
            _lock.ExitReadLock();
        }
    }
}
