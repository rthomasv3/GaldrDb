using System;
using System.Collections.Generic;

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

    /// <summary>
    /// Gets the encoded index key bytes for all values in an In filter.
    /// </summary>
    /// <returns>Encoded key bytes for each value, or null for non-In filters.</returns>
    IEnumerable<byte[]> GetAllIndexKeyBytes();

    /// <summary>
    /// Gets the filter value as a boxed object.
    /// </summary>
    /// <returns>The filter value, or null for filters without a single value.</returns>
    object GetFilterValue();

    /// <summary>
    /// Gets the minimum value for range filters (Between).
    /// </summary>
    /// <returns>The minimum value, or null for non-range filters.</returns>
    object GetRangeMinValue();

    /// <summary>
    /// Gets the maximum value for range filters (Between).
    /// </summary>
    /// <returns>The maximum value, or null for non-range filters.</returns>
    object GetRangeMaxValue();

    /// <summary>
    /// Gets the values for an In filter when the field type is Int32.
    /// </summary>
    /// <returns>The int values, or null for non-In filters or non-Int32 fields.</returns>
    IReadOnlyList<int> GetInValuesAsInt32();

    /// <summary>
    /// Gets the size needed to encode this filter's value in compound key format.
    /// </summary>
    /// <returns>The number of bytes required.</returns>
    int GetCompoundEncodedSize();

    /// <summary>
    /// Encodes this filter's value directly to a buffer in compound key format.
    /// This method avoids boxing by using type-specific encoding.
    /// </summary>
    /// <param name="buffer">The buffer to write to.</param>
    /// <param name="offset">The offset to start writing at.</param>
    /// <returns>The number of bytes written.</returns>
    int EncodeCompoundFieldTo(byte[] buffer, int offset);

    /// <summary>
    /// Gets the size needed to encode this filter's max value in compound key format.
    /// For Between filters, this is the max value. For other filters, same as GetCompoundEncodedSize.
    /// </summary>
    /// <returns>The number of bytes required.</returns>
    int GetCompoundEncodedSizeMax();

    /// <summary>
    /// Encodes this filter's max value directly to a buffer in compound key format.
    /// For Between filters, this encodes the max value. For other filters, same as EncodeCompoundFieldTo.
    /// </summary>
    /// <param name="buffer">The buffer to write to.</param>
    /// <param name="offset">The offset to start writing at.</param>
    /// <returns>The number of bytes written.</returns>
    int EncodeCompoundFieldToMax(byte[] buffer, int offset);

    /// <summary>
    /// Gets the size needed to encode this filter's value as a prefix in compound key format.
    /// For StartsWith filters on strings, this excludes the null terminator.
    /// For other filters, same as GetCompoundEncodedSize.
    /// </summary>
    /// <returns>The number of bytes required.</returns>
    int GetCompoundEncodedSizeForPrefix();

    /// <summary>
    /// Encodes this filter's value as a prefix directly to a buffer in compound key format.
    /// For StartsWith filters on strings, this excludes the null terminator.
    /// For other filters, same as EncodeCompoundFieldTo.
    /// </summary>
    /// <param name="buffer">The buffer to write to.</param>
    /// <param name="offset">The offset to start writing at.</param>
    /// <returns>The number of bytes written.</returns>
    int EncodeCompoundFieldToForPrefix(byte[] buffer, int offset);
}
