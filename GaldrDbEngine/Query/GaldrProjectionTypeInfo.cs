using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrJson;

namespace GaldrDbEngine.Query;

public sealed class GaldrProjectionTypeInfo<TProjection, TSource> : IGaldrProjectionTypeInfo
    where TProjection : IProjectionOf<TSource>
{
    private readonly Func<TSource, TProjection> _converter;
    private readonly Func<TSource, int> _sourceIdGetter;

    public Type DocumentType { get; }
    public Type SourceType { get; }
    public string CollectionName { get; }
    public IReadOnlyList<string> IndexedFieldNames { get; }
    public IReadOnlyList<string> UniqueIndexFieldNames { get; }

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

    public object DeserializeSource(string json, IGaldrJsonSerializer serializer, GaldrJsonOptions options)
    {
        return serializer.Deserialize<TSource>(json, options);
    }

    public object ConvertToProjection(object source)
    {
        return _converter((TSource)source);
    }

    public int GetSourceId(object source)
    {
        return _sourceIdGetter((TSource)source);
    }

    public void ExtractIndexedFieldsFrom(object document, IndexFieldWriter writer)
    {
        throw new NotSupportedException("Projections do not support index field extraction");
    }

    public int GetIdFrom(object document)
    {
        throw new NotSupportedException("Projections do not support direct ID access");
    }
}
