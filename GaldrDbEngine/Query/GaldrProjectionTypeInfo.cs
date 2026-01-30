using System;
using System.Collections.Generic;
using GaldrJson;

namespace GaldrDbEngine.Query;

/// <summary>
/// Type metadata for a projection type. Created by source generators.
/// </summary>
/// <typeparam name="TProjection">The projection type.</typeparam>
/// <typeparam name="TSource">The source document type.</typeparam>
public sealed class GaldrProjectionTypeInfo<TProjection, TSource> : IGaldrProjectionTypeInfo
    where TProjection : IProjectionOf<TSource>
{
    private static readonly IReadOnlyList<CompoundIndexInfo> _emptyCompoundIndexes = Array.Empty<CompoundIndexInfo>();
    private readonly Func<TSource, TProjection> _converter;
    private readonly Func<TSource, int> _sourceIdGetter;

    /// <inheritdoc/>
    public Type DocumentType { get; }

    /// <inheritdoc/>
    public Type SourceType { get; }

    /// <inheritdoc/>
    public string CollectionName { get; }

    /// <inheritdoc/>
    public IReadOnlyList<string> IndexedFieldNames { get; }

    /// <inheritdoc/>
    public IReadOnlyList<string> UniqueIndexFieldNames { get; }

    /// <inheritdoc/>
    public IReadOnlyList<CompoundIndexInfo> CompoundIndexes => _emptyCompoundIndexes;

    /// <summary>
    /// Creates a new projection type info instance.
    /// </summary>
    /// <param name="collectionName">The collection name of the source type.</param>
    /// <param name="indexedFieldNames">Names of indexed fields on the source.</param>
    /// <param name="uniqueIndexFieldNames">Names of unique indexed fields on the source.</param>
    /// <param name="converter">Delegate to convert source to projection.</param>
    /// <param name="sourceIdGetter">Delegate to get the source document ID.</param>
    public GaldrProjectionTypeInfo(
        string collectionName,
        IReadOnlyList<string> indexedFieldNames,
        IReadOnlyList<string> uniqueIndexFieldNames,
        Func<TSource, TProjection> converter,
        Func<TSource, int> sourceIdGetter)
    {
        DocumentType = typeof(TProjection);
        SourceType = typeof(TSource);
        CollectionName = collectionName;
        IndexedFieldNames = indexedFieldNames;
        UniqueIndexFieldNames = uniqueIndexFieldNames;
        _converter = converter;
        _sourceIdGetter = sourceIdGetter;
    }

    /// <inheritdoc/>
    public object DeserializeSource(string json, IGaldrJsonSerializer serializer, GaldrJsonOptions options)
    {
        return serializer.Deserialize<TSource>(json, options);
    }

    /// <inheritdoc/>
    public object ConvertToProjection(object source)
    {
        return _converter((TSource)source);
    }

    /// <inheritdoc/>
    public int GetSourceId(object source)
    {
        return _sourceIdGetter((TSource)source);
    }

    /// <inheritdoc/>
    public void ExtractIndexedFieldsFrom(object document, IndexFieldWriter writer)
    {
        throw new NotSupportedException("Projections do not support index field extraction");
    }

    /// <inheritdoc/>
    public int GetIdFrom(object document)
    {
        throw new NotSupportedException("Projections do not support direct ID access");
    }
}
