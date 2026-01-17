using System;
using System.Collections.Generic;

namespace GaldrDbEngine.MVCC;

/// <summary>
/// Contains statistics from a garbage collection operation.
/// </summary>
public sealed class GarbageCollectionResult
{
    private static readonly IReadOnlyList<CollectableVersion> EmptyList = Array.Empty<CollectableVersion>();

    internal static readonly GarbageCollectionResult Empty = new GarbageCollectionResult(0, 0, EmptyList, 0);

    /// <summary>Number of old document versions that were collected.</summary>
    public int VersionsCollected { get; }

    /// <summary>Number of documents that were processed.</summary>
    public int DocumentsProcessed { get; }

    internal IReadOnlyList<CollectableVersion> CollectableVersions { get; }

    /// <summary>Number of pages that were compacted.</summary>
    public int PagesCompacted { get; }

    internal GarbageCollectionResult(int versionsCollected, int documentsProcessed, IReadOnlyList<CollectableVersion> collectableVersions)
        : this(versionsCollected, documentsProcessed, collectableVersions, 0)
    {
    }

    internal GarbageCollectionResult(int versionsCollected, int documentsProcessed, IReadOnlyList<CollectableVersion> collectableVersions, int pagesCompacted)
    {
        VersionsCollected = versionsCollected;
        DocumentsProcessed = documentsProcessed;
        CollectableVersions = collectableVersions;
        PagesCompacted = pagesCompacted;
    }
}
