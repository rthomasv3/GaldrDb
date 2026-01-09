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
        TxId oldestSnapshot = _txManager.GetOldestActiveSnapshot();

        if (oldestSnapshot == TxId.MaxValue)
        {
            oldestSnapshot = _txManager.LastCommittedTxId;
        }

        int documentsProcessed = 0;
        List<CollectableVersion> allCollectableVersions = new List<CollectableVersion>();

        List<CollectionVersions> allCollections = _versionIndex.GetAllCollectionsForGC();

        foreach (CollectionVersions collectionVersions in allCollections)
        {
            foreach (DocumentVersionChain chain in collectionVersions.Chains)
            {
                documentsProcessed++;
                List<CollectableVersion> chainCollectables = IdentifyCollectableVersions(chain, oldestSnapshot);
                allCollectableVersions.AddRange(chainCollectables);
            }
        }

        return new GarbageCollectionResult(allCollectableVersions.Count, documentsProcessed, allCollectableVersions);
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

    private List<CollectableVersion> IdentifyCollectableVersions(DocumentVersionChain chain, TxId oldestSnapshot)
    {
        List<CollectableVersion> collectables = new List<CollectableVersion>();
        DocumentVersion current = chain.Head;
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
                    chain.CollectionName,
                    chain.DocumentId,
                    previous,
                    current,
                    current.Location);
                collectables.Add(collectable);
            }
            else
            {
                previous = current;
            }

            current = current.PreviousVersion;
        }

        return collectables;
    }
}
