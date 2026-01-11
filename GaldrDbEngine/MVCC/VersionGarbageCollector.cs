using System.Collections.Generic;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

public sealed class VersionGarbageCollector
{
    private readonly VersionIndex _versionIndex;
    private readonly TransactionManager _txManager;

    public VersionGarbageCollector(VersionIndex versionIndex, TransactionManager txManager)
    {
        _versionIndex = versionIndex;
        _txManager = txManager;
    }

    public GarbageCollectionResult Collect()
    {
        // Early exit: if no documents have multiple versions, nothing to collect
        if (_versionIndex.MultiVersionDocumentCount == 0)
        {
            return GarbageCollectionResult.Empty;
        }

        TxId oldestSnapshot = _txManager.GetOldestActiveSnapshot();

        if (oldestSnapshot == TxId.MaxValue)
        {
            oldestSnapshot = _txManager.LastCommittedTxId;
        }

        List<CollectableVersion> collectableVersions = new List<CollectableVersion>();

        // Iterate directly without wrapper objects, skip single-version chains
        _versionIndex.CollectGarbageVersions(oldestSnapshot, collectableVersions);

        return new GarbageCollectionResult(collectableVersions.Count, _versionIndex.MultiVersionDocumentCount, collectableVersions);
    }

    public void UnlinkVersions(IReadOnlyList<CollectableVersion> collectableVersions)
    {
        foreach (CollectableVersion collectable in collectableVersions)
        {
            if (collectable.Previous != null)
            {
                _versionIndex.UnlinkVersion(
                    collectable.CollectionName,
                    collectable.DocumentId,
                    collectable.Previous,
                    collectable.ToRemove);
            }
        }
    }
}
