using System;

namespace GaldrDbEngine.Query;

/// <summary>
/// Represents a filter condition on a document field.
/// </summary>
public interface IFieldFilter
{
    /// <summary>The name of the field being filtered.</summary>
    string FieldName { get; }

    /// <summary>The data type of the field.</summary>
    GaldrFieldType FieldType { get; }

    /// <summary>Whether this field has a secondary index.</summary>
    bool IsIndexed { get; }

    /// <summary>The comparison operation.</summary>
    FieldOp Operation { get; }

    /// <summary>The document type this filter applies to.</summary>
    Type DocumentType { get; }

    /// <summary>
    /// Evaluates the filter against a document.
    /// </summary>
    /// <param name="document">The document to evaluate.</param>
    /// <returns>True if the document matches the filter.</returns>
    bool Evaluate(object document);

    /// <summary>
    /// Gets the encoded index key bytes for the filter value.
    /// </summary>
    /// <returns>The encoded key bytes.</returns>
    byte[] GetIndexKeyBytes();

    /// <summary>
    /// Gets the encoded index key bytes for the end of a range filter.
    /// </summary>
    /// <returns>The encoded end key bytes, or null for non-range filters.</returns>
    byte[] GetIndexKeyEndBytes();
}
