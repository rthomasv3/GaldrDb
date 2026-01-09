using System.Collections.Generic;

namespace GaldrDbEngine.MVCC;

public sealed class GarbageCollectionResult
{
    public int VersionsCollected { get; }
    public int DocumentsProcessed { get; }
    public IReadOnlyList<CollectableVersion> CollectableVersions { get; }

    public GarbageCollectionResult(int versionsCollected, int documentsProcessed, IReadOnlyList<CollectableVersion> collectableVersions)
    {
        VersionsCollected = versionsCollected;
        DocumentsProcessed = documentsProcessed;
        CollectableVersions = collectableVersions;
    }
}
