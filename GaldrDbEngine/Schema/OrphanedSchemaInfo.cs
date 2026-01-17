using System.Collections.Generic;

namespace GaldrDbEngine.Schema;

/// <summary>
/// Information about collections and indexes that exist in the database but are no longer defined in code.
/// </summary>
public class OrphanedSchemaInfo
{
    /// <summary>Names of collections that are no longer defined in code.</summary>
    public IReadOnlyList<string> Collections { get; }

    /// <summary>Indexes that are no longer defined in code.</summary>
    public IReadOnlyList<OrphanedIndexInfo> Indexes { get; }

    /// <summary>
    /// Creates a new orphaned schema info.
    /// </summary>
    /// <param name="collections">The orphaned collection names.</param>
    /// <param name="indexes">The orphaned indexes.</param>
    internal OrphanedSchemaInfo(IReadOnlyList<string> collections, IReadOnlyList<OrphanedIndexInfo> indexes)
    {
        Collections = collections;
        Indexes = indexes;
    }

    /// <summary>Whether there are any orphaned collections or indexes.</summary>
    public bool HasOrphans
    {
        get { return Collections.Count > 0 || Indexes.Count > 0; }
    }
}
