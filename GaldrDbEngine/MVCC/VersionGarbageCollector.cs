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

        // If no active transactions, use LastCommittedTxId as the cutoff
        if (oldestSnapshot == TxId.MaxValue)
        {
            oldestSnapshot = _txManager.LastCommittedTxId;
        }

        int versionsCollected = 0;
        int documentsProcessed = 0;

        List<CollectionVersions> allCollections = _versionIndex.GetAllCollectionsForGC();

        foreach (CollectionVersions collectionVersions in allCollections)
        {
            foreach (DocumentVersionChain chain in collectionVersions.Chains)
            {
                documentsProcessed++;
                versionsCollected += CollectVersionChain(chain, oldestSnapshot);
            }
        }

        return new GarbageCollectionResult(versionsCollected, documentsProcessed);
    }

    private int CollectVersionChain(DocumentVersionChain chain, TxId oldestSnapshot)
    {
        int collected = 0;
        DocumentVersion current = chain.Head;
        DocumentVersion previous = null;
        bool foundVisibleVersion = false;

        while (current != null)
        {
            bool canCollect = false;

            if (foundVisibleVersion)
            {
                // We've already found a version visible to oldestSnapshot
                // This older version can be collected if it was superseded before oldestSnapshot
                if (current.DeletedBy != TxId.MaxValue && current.DeletedBy < oldestSnapshot)
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
                else if (current.DeletedBy != TxId.MaxValue && current.DeletedBy < oldestSnapshot)
                {
                    // This version was deleted before any active snapshot can see it
                    canCollect = true;
                }
            }

            if (canCollect)
            {
                if (previous != null)
                {
                    _versionIndex.UnlinkVersion(chain.CollectionName, chain.DocumentId, previous, current);
                }
                collected++;
            }
            else
            {
                previous = current;
            }

            current = current.PreviousVersion;
        }

        return collected;
    }
}
