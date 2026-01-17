namespace GaldrDbEngine.Schema;

/// <summary>
/// Information about an index that exists in the database but is no longer defined in code.
/// </summary>
public class OrphanedIndexInfo
{
    /// <summary>The collection containing the orphaned index.</summary>
    public string CollectionName { get; }

    /// <summary>The field name of the orphaned index.</summary>
    public string FieldName { get; }

    /// <summary>
    /// Creates a new orphaned index info.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="fieldName">The field name.</param>
    internal OrphanedIndexInfo(string collectionName, string fieldName)
    {
        CollectionName = collectionName;
        FieldName = fieldName;
    }
}
