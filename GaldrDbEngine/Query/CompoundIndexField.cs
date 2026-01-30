namespace GaldrDbEngine.Query;

/// <summary>
/// Represents a single field within a compound index.
/// </summary>
public readonly struct CompoundIndexField
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
    /// Creates a new compound index field.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="fieldType">The field type.</param>
    public CompoundIndexField(string fieldName, GaldrFieldType fieldType)
    {
        FieldName = fieldName;
        FieldType = fieldType;
    }
}
