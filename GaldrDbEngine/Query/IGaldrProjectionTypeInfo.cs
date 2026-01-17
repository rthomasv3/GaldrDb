using System;
using GaldrJson;

namespace GaldrDbEngine.Query;

/// <summary>
/// Provides type metadata for a projection type. Implemented by source-generated types.
/// </summary>
public interface IGaldrProjectionTypeInfo : IGaldrTypeInfo
{
    /// <summary>The CLR type of the source document.</summary>
    Type SourceType { get; }

    /// <summary>Deserializes JSON into the source document type.</summary>
    /// <param name="json">The JSON string.</param>
    /// <param name="serializer">The serializer to use.</param>
    /// <param name="options">Serialization options.</param>
    /// <returns>The deserialized source document.</returns>
    object DeserializeSource(string json, IGaldrJsonSerializer serializer, GaldrJsonOptions options);

    /// <summary>Converts a source document to the projection type.</summary>
    /// <param name="source">The source document.</param>
    /// <returns>The projection instance.</returns>
    object ConvertToProjection(object source);

    /// <summary>Gets the document ID from a source document.</summary>
    /// <param name="source">The source document.</param>
    /// <returns>The document ID.</returns>
    int GetSourceId(object source);
}
