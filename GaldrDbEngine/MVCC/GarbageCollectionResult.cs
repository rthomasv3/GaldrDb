using System;
using System.Collections.Generic;

namespace GaldrDbEngine.MVCC;

public sealed class GarbageCollectionResult
{
    private static readonly IReadOnlyList<CollectableVersion> EmptyList = Array.Empty<CollectableVersion>();
    public static readonly GarbageCollectionResult Empty = new GarbageCollectionResult(0, 0, EmptyList, 0);

    public int VersionsCollected { get; }
    public int DocumentsProcessed { get; }
    public IReadOnlyList<CollectableVersion> CollectableVersions { get; }
    public int PagesCompacted { get; }

    public GarbageCollectionResult(int versionsCollected, int documentsProcessed, IReadOnlyList<CollectableVersion> collectableVersions)
        : this(versionsCollected, documentsProcessed, collectableVersions, 0)
    {
    }

    public GarbageCollectionResult(int versionsCollected, int documentsProcessed, IReadOnlyList<CollectableVersion> collectableVersions, int pagesCompacted)
    {
        VersionsCollected = versionsCollected;
        DocumentsProcessed = documentsProcessed;
        CollectableVersions = collectableVersions;
        PagesCompacted = pagesCompacted;
    }
}
