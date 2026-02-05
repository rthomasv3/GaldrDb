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
        ulong oldestSnapshotCSN = _txManager.GetOldestActiveSnapshotCSN();

        if (oldestSnapshotCSN == ulong.MaxValue)
        {
            oldestSnapshotCSN = _txManager.GetCurrentCSN();
        }

        List<CollectableVersion> collectableVersions = new List<CollectableVersion>();

        // Iterate directly without wrapper objects, skip single-version chains
        _versionIndex.CollectGarbageVersions(oldestSnapshotCSN, collectableVersions);

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
