using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// Represents a typed field within a collection property for use in any-match queries.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
/// <typeparam name="TElement">The collection element type.</typeparam>
/// <typeparam name="TField">The field type within the element.</typeparam>
public sealed class GaldrCollectionField<TDocument, TElement, TField>
{
    /// <summary>The name of the field (including collection path, e.g., "Addresses.City").</summary>
    public string FieldName { get; }

    /// <summary>The data type of the field.</summary>
    public GaldrFieldType FieldType { get; }

    /// <summary>Whether this field has a secondary index (always false for collection fields).</summary>
    public bool IsIndexed { get; }

    /// <summary>Function to access the collection from a document.</summary>
    public Func<TDocument, IEnumerable<TElement>> CollectionAccessor { get; }

    /// <summary>Function to access the field value from a collection element.</summary>
    public Func<TElement, TField> ElementAccessor { get; }

    /// <summary>
    /// Creates a new GaldrCollectionField.
    /// </summary>
    /// <param name="fieldName">The field name including collection path.</param>
    /// <param name="fieldType">The field type.</param>
    /// <param name="isIndexed">Whether the field is indexed (always false for collections).</param>
    /// <param name="collectionAccessor">Function to access the collection from a document.</param>
    /// <param name="elementAccessor">Function to access the field value from an element.</param>
    public GaldrCollectionField(
        string fieldName,
        GaldrFieldType fieldType,
        bool isIndexed,
        Func<TDocument, IEnumerable<TElement>> collectionAccessor,
        Func<TElement, TField> elementAccessor)
    {
        FieldName = fieldName;
        FieldType = fieldType;
        IsIndexed = isIndexed;
        CollectionAccessor = collectionAccessor;
        ElementAccessor = elementAccessor;
    }
}
