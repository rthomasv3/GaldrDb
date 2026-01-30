using GaldrDbEngine.Query;

namespace GaldrDbEngine.Storage;

/// <summary>
/// Represents a single field within an index definition.
/// </summary>
internal readonly struct IndexField
{
    /// <summary>
    /// The name of the field.
    /// </summary>
    public string FieldName { get; }

    /// <summary>
    /// The type of the field for encoding.
    /// </summary>
    public GaldrFieldType FieldType { get; }

    /// <summary>
    /// Creates a new index field.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="fieldType">The field type.</param>
    public IndexField(string fieldName, GaldrFieldType fieldType)
    {
        FieldName = fieldName;
        FieldType = fieldType;
    }
}
