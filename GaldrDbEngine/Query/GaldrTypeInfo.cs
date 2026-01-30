using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// Type metadata for a document type. Created by source generators.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class GaldrTypeInfo<T> : IGaldrTypeInfo
{
    private static readonly IReadOnlyList<CompoundIndexInfo> _emptyCompoundIndexes = Array.Empty<CompoundIndexInfo>();

    /// <inheritdoc/>
    public Type DocumentType { get; }

    /// <inheritdoc/>
    public string CollectionName { get; }

    /// <inheritdoc/>
    public IReadOnlyList<string> IndexedFieldNames { get; }

    /// <inheritdoc/>
    public IReadOnlyList<string> UniqueIndexFieldNames { get; }

    /// <inheritdoc/>
    public IReadOnlyList<CompoundIndexInfo> CompoundIndexes { get; }

    /// <summary>Delegate to set the document ID.</summary>
    public Action<T, int> IdSetter { get; }

    /// <summary>Delegate to get the document ID.</summary>
    public Func<T, int> IdGetter { get; }

    /// <summary>Delegate to extract indexed field values.</summary>
    public Action<T, IndexFieldWriter> ExtractIndexedFields { get; }

    /// <summary>
    /// Creates a new type info instance.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="indexedFieldNames">Names of indexed fields.</param>
    /// <param name="uniqueIndexFieldNames">Names of unique indexed fields.</param>
    /// <param name="idSetter">Delegate to set the ID.</param>
    /// <param name="idGetter">Delegate to get the ID.</param>
    /// <param name="extractIndexedFields">Delegate to extract indexed fields.</param>
    public GaldrTypeInfo(
        string collectionName,
        IReadOnlyList<string> indexedFieldNames,
        IReadOnlyList<string> uniqueIndexFieldNames,
        Action<T, int> idSetter,
        Func<T, int> idGetter,
        Action<T, IndexFieldWriter> extractIndexedFields)
    {
        DocumentType = typeof(T);
        CollectionName = collectionName;
        IndexedFieldNames = indexedFieldNames;
        UniqueIndexFieldNames = uniqueIndexFieldNames;
        CompoundIndexes = _emptyCompoundIndexes;
        IdSetter = idSetter;
        IdGetter = idGetter;
        ExtractIndexedFields = extractIndexedFields;
    }

    /// <summary>
    /// Creates a new type info instance with compound indexes.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="indexedFieldNames">Names of indexed fields.</param>
    /// <param name="uniqueIndexFieldNames">Names of unique indexed fields.</param>
    /// <param name="compoundIndexes">Compound index definitions.</param>
    /// <param name="idSetter">Delegate to set the ID.</param>
    /// <param name="idGetter">Delegate to get the ID.</param>
    /// <param name="extractIndexedFields">Delegate to extract indexed fields.</param>
    public GaldrTypeInfo(
        string collectionName,
        IReadOnlyList<string> indexedFieldNames,
        IReadOnlyList<string> uniqueIndexFieldNames,
        IReadOnlyList<CompoundIndexInfo> compoundIndexes,
        Action<T, int> idSetter,
        Func<T, int> idGetter,
        Action<T, IndexFieldWriter> extractIndexedFields)
    {
        DocumentType = typeof(T);
        CollectionName = collectionName;
        IndexedFieldNames = indexedFieldNames;
        UniqueIndexFieldNames = uniqueIndexFieldNames;
        CompoundIndexes = compoundIndexes ?? _emptyCompoundIndexes;
        IdSetter = idSetter;
        IdGetter = idGetter;
        ExtractIndexedFields = extractIndexedFields;
    }

    /// <inheritdoc/>
    public void ExtractIndexedFieldsFrom(object document, IndexFieldWriter writer)
    {
        ExtractIndexedFields((T)document, writer);
    }

    /// <inheritdoc/>
    public int GetIdFrom(object document)
    {
        return IdGetter((T)document);
    }
}
