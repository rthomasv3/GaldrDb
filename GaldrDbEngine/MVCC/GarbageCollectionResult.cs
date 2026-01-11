using System;
using System.Collections.Generic;

namespace GaldrDbEngine.MVCC;

public sealed class GarbageCollectionResult
{
    private static readonly IReadOnlyList<CollectableVersion> EmptyList = Array.Empty<CollectableVersion>();
    public static readonly GarbageCollectionResult Empty = new GarbageCollectionResult(0, 0, EmptyList);

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
