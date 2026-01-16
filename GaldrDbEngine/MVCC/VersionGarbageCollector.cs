using System.Collections.Generic;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

internal sealed class VersionGarbageCollector
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
            _versionIndex.UnlinkVersion(
                collectable.CollectionName,
                collectable.DocumentId,
                collectable.Previous,
                collectable.ToRemove);
        }
    }
}
