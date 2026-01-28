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

    public VersionScanner(
        GaldrDb db,
        VersionIndex versionIndex,
        TxId snapshotTxId,
        SecondaryIndexScanner indexScanner)
    {
        _db = db;
        _versionIndex = versionIndex;
        _snapshotTxId = snapshotTxId;
        _indexScanner = indexScanner;
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
            List<int> docIds = _db.SearchDocIdRange(collectionName, rangeSpec.StartDocId, rangeSpec.EndDocId, rangeSpec.IncludeStart, rangeSpec.IncludeEnd);
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
            List<int> docIds = await _db.SearchDocIdRangeAsync(collectionName, rangeSpec.StartDocId, rangeSpec.EndDocId, rangeSpec.IncludeStart, rangeSpec.IncludeEnd, cancellationToken).ConfigureAwait(false);
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
        int count = collection?.DocumentCount ?? 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName == collectionName)
            {
                WriteSetEntry entry = kvp.Value;

                if (entry.Operation == WriteOperation.Insert)
                {
                    count++;
                }
                else if (entry.Operation == WriteOperation.Delete)
                {
                    count--;
                }
            }
        }

        return count;
    }

    public bool HasAnyUnfiltered(string collectionName, CollectionEntry collection, Transaction transaction)
    {
        int snapshotCount = collection?.DocumentCount ?? 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = transaction.GetWriteSet();
        bool hasAny = false;

        if (snapshotCount > 0)
        {
            List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

            foreach (DocumentVersion version in visibleVersions)
            {
                DocumentKey key = new DocumentKey(collectionName, version.DocumentId);
                if (!writeSet.TryGetValue(key, out WriteSetEntry entry) || entry.Operation != WriteOperation.Delete)
                {
                    hasAny = true;
                    break;
                }
            }
        }

        if (!hasAny)
        {
            foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
            {
                if (kvp.Key.CollectionName == collectionName && kvp.Value.Operation == WriteOperation.Insert)
                {
                    hasAny = true;
                    break;
                }
            }
        }

        return hasAny;
    }
}
