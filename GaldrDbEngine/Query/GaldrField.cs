using System;

namespace GaldrDbEngine.Query;

/// <summary>
/// Represents a typed field on a document for use in queries.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TField">The field type.</typeparam>
public sealed class GaldrField<TDocument, TField>
{
    /// <summary>The name of the field.</summary>
    public string FieldName { get; }

    /// <summary>The data type of the field.</summary>
    public GaldrFieldType FieldType { get; }

    /// <summary>Whether this field has a secondary index.</summary>
    public bool IsIndexed { get; }

    /// <summary>Function to access the field value from a document.</summary>
    public Func<TDocument, TField> Accessor { get; }

    /// <summary>
    /// Creates a new GaldrField.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="fieldType">The field type.</param>
    /// <param name="isIndexed">Whether the field is indexed.</param>
    /// <param name="accessor">Function to access the field value.</param>
    public GaldrField(
        string fieldName,
        GaldrFieldType fieldType,
        bool isIndexed,
        Func<TDocument, TField> accessor)
    {
        FieldName = fieldName;
        FieldType = fieldType;
        IsIndexed = isIndexed;
        Accessor = accessor;
    }
}
