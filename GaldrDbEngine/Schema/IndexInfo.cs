using GaldrDbEngine.Query;

namespace GaldrDbEngine.Schema;

/// <summary>
/// Public information about an index on a collection.
/// </summary>
public sealed class IndexInfo
{
    /// <summary>The name of the indexed field.</summary>
    public string FieldName { get; }

    /// <summary>The data type of the indexed field.</summary>
    public GaldrFieldType FieldType { get; }

    /// <summary>Whether this index enforces uniqueness.</summary>
    public bool IsUnique { get; }

    internal IndexInfo(string fieldName, GaldrFieldType fieldType, bool isUnique)
    {
        FieldName = fieldName;
        FieldType = fieldType;
        IsUnique = isUnique;
    }
}
