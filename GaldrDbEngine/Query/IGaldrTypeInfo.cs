using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// Provides type metadata for a document type. Implemented by source-generated types.
/// </summary>
public interface IGaldrTypeInfo
{
    /// <summary>The CLR type of the document.</summary>
    Type DocumentType { get; }

    /// <summary>The collection name for this document type.</summary>
    string CollectionName { get; }

    /// <summary>Names of fields that have secondary indexes.</summary>
    IReadOnlyList<string> IndexedFieldNames { get; }

    /// <summary>Names of fields that have unique indexes.</summary>
    IReadOnlyList<string> UniqueIndexFieldNames { get; }

    /// <summary>Metadata for compound indexes on this type.</summary>
    IReadOnlyList<CompoundIndexInfo> CompoundIndexes { get; }

    /// <summary>Extracts indexed field values from a document.</summary>
    /// <param name="document">The document to extract from.</param>
    /// <param name="writer">The writer to output field values to.</param>
    void ExtractIndexedFieldsFrom(object document, IndexFieldWriter writer);

    /// <summary>Gets the document ID from a document instance.</summary>
    /// <param name="document">The document.</param>
    /// <returns>The document ID.</returns>
    int GetIdFrom(object document);
}
