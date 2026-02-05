using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query.Planning;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query.Execution;

internal sealed class VersionScanner
{
    private readonly GaldrDb _db;
    private readonly VersionIndex _versionIndex;
    private readonly TxId _snapshotTxId;
    private readonly SecondaryIndexScanner _indexScanner;
    private readonly TransactionContext _context;

    public VersionScanner(
        GaldrDb db,
        VersionIndex versionIndex,
        TxId snapshotTxId,
        SecondaryIndexScanner indexScanner,
        TransactionContext context)
    {
        _db = db;
        _versionIndex = versionIndex;
        _snapshotTxId = snapshotTxId;
        _indexScanner = indexScanner;
        _context = context;
    }

    public List<DocumentVersion> GetVersionsForPlan(string collectionName, QueryExecutionPlan plan)
    {
        List<DocumentVersion> versions;

        if (plan.PlanType == QueryPlanType.PrimaryKeyScan)
        {
            List<int> docIds = _versionIndex.GetDocumentIds(collectionName);
            if (plan.ScanDirection == ScanDirection.Descending)
            {
                docIds.Reverse();
            }
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            PrimaryKeyRangeSpec rangeSpec = plan.PrimaryKeyRange;
            List<int> docIds = _db.SearchDocIdRange(collectionName, rangeSpec.StartDocId, rangeSpec.EndDocId, rangeSpec.IncludeStart, rangeSpec.IncludeEnd, _context);
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.PrimaryKeyMultiPoint)
        {
            PrimaryKeyMultiPointSpec multiPointSpec = plan.PrimaryKeyMultiPoint;
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, multiPointSpec.DocIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
        {
            List<SecondaryIndexEntry> entries = _indexScanner.GetEntries(plan.SecondaryIndex);
            if (plan.SecondaryIndex.Direction == ScanDirection.Descending)
            {
                entries.Reverse();
            }
            List<int> docIds = SecondaryIndexScanner.ExtractDocIds(entries);
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else
        {
            versions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);
        }

        return versions;
    }

    public async Task<List<DocumentVersion>> GetVersionsForPlanAsync(
        string collectionName,
        QueryExecutionPlan plan,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions;

        if (plan.PlanType == QueryPlanType.PrimaryKeyScan)
        {
            List<int> docIds = _versionIndex.GetDocumentIds(collectionName);
            if (plan.ScanDirection == ScanDirection.Descending)
            {
                docIds.Reverse();
            }
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            PrimaryKeyRangeSpec rangeSpec = plan.PrimaryKeyRange;
            List<int> docIds = await _db.SearchDocIdRangeAsync(collectionName, rangeSpec.StartDocId, rangeSpec.EndDocId, rangeSpec.IncludeStart, rangeSpec.IncludeEnd, _context, cancellationToken).ConfigureAwait(false);
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.PrimaryKeyMultiPoint)
        {
            PrimaryKeyMultiPointSpec multiPointSpec = plan.PrimaryKeyMultiPoint;
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, multiPointSpec.DocIds, _snapshotTxId);
        }
        else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
        {
            List<SecondaryIndexEntry> entries = _indexScanner.GetEntries(plan.SecondaryIndex);
            if (plan.SecondaryIndex.Direction == ScanDirection.Descending)
            {
                entries.Reverse();
            }
            List<int> docIds = SecondaryIndexScanner.ExtractDocIds(entries);
            versions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, docIds, _snapshotTxId);
        }
        else
        {
            versions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);
        }

        return versions;
    }

    public int GetUnfilteredCount(string collectionName, CollectionEntry collection, Transaction transaction)
    {
        // Get visible versions directly from the version index to ensure consistency.
        // Using DocumentCount would create a race condition: during commit, the version index
        // is updated before DocumentCount, so a query could see a document marked as deleted
        // in the version index while DocumentCount still includes it.
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        // Build a set of visible document IDs for quick lookup
        HashSet<int> visibleDocIds = new HashSet<int>();
        foreach (DocumentVersion version in visibleVersions)
        {
            visibleDocIds.Add(version.DocumentId);
        }

        int count = visibleVersions.Count;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName == collectionName)
            {
                WriteSetEntry entry = kvp.Value;
                int docId = kvp.Key.DocId;

                if (entry.Operation == WriteOperation.Insert)
                {
                    // Only count if document is not already visible (new insert)
                    if (!visibleDocIds.Contains(docId))
                    {
                        count++;
                    }
                }
                else if (entry.Operation == WriteOperation.Delete)
                {
                    // Only subtract if document is currently visible (being deleted)
                    if (visibleDocIds.Contains(docId))
                    {
                        count--;
                    }
                }
            }
        }

        return count;
    }

    public bool HasAnyUnfiltered(string collectionName, CollectionEntry collection, Transaction transaction)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = transaction.GetWriteSet();
        bool hasAny = false;

        // Check for any inserts in write set first (quick check)
        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName == collectionName && kvp.Value.Operation == WriteOperation.Insert)
            {
                hasAny = true;
                break;
            }
        }

        // Check visible versions from the version index
        if (!hasAny)
        {
            List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

            foreach (DocumentVersion version in visibleVersions)
            {
                DocumentKey key = new DocumentKey(collectionName, version.DocumentId);
                // Found a visible document that is not being deleted in this transaction
                if (!writeSet.TryGetValue(key, out WriteSetEntry entry) || entry.Operation != WriteOperation.Delete)
                {
                    hasAny = true;
                    break;
                }
            }
        }

        return hasAny;
    }
}
