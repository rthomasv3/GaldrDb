using System.Collections.Generic;

namespace GaldrDbEngine.Schema;

/// <summary>
/// Public information about a collection in the database.
/// </summary>
public sealed class CollectionInfo
{
    /// <summary>The name of the collection.</summary>
    public string Name { get; }

    /// <summary>The number of documents in the collection.</summary>
    public int DocumentCount { get; }

    /// <summary>The indexes defined on this collection.</summary>
    public IReadOnlyList<IndexInfo> Indexes { get; }

    internal CollectionInfo(string name, int documentCount, IReadOnlyList<IndexInfo> indexes)
    {
        Name = name;
        DocumentCount = documentCount;
        Indexes = indexes;
    }
}
