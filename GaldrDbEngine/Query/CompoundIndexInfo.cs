using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// Metadata about a compound index declared on a document type.
/// </summary>
public sealed class CompoundIndexInfo
{
    /// <summary>
    /// The index name, formed by joining field names with underscores (e.g., "Status_CreatedDate").
    /// </summary>
    public string IndexName { get; }

    /// <summary>
    /// The fields that make up this index, in order.
    /// </summary>
    public IReadOnlyList<CompoundIndexField> Fields { get; }

    /// <summary>
    /// Whether this index enforces uniqueness on the combination of field values.
    /// </summary>
    public bool IsUnique { get; }

    /// <summary>
    /// Creates a new compound index info.
    /// </summary>
    /// <param name="indexName">The index name.</param>
    /// <param name="fields">The fields in index order.</param>
    /// <param name="isUnique">Whether uniqueness is enforced.</param>
    public CompoundIndexInfo(string indexName, IReadOnlyList<CompoundIndexField> fields, bool isUnique)
    {
        IndexName = indexName;
        Fields = fields;
        IsUnique = isUnique;
    }
}
