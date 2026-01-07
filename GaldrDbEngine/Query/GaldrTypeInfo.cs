using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class GaldrTypeInfo<T> : IGaldrTypeInfo
{
    public Type DocumentType { get; }
    public string CollectionName { get; }
    public IReadOnlyList<string> IndexedFieldNames { get; }
    public IReadOnlyList<string> UniqueIndexFieldNames { get; }
    public Action<T, int> IdSetter { get; }
    public Func<T, int> IdGetter { get; }
    public Action<T, IndexFieldWriter> ExtractIndexedFields { get; }

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
        IdSetter = idSetter;
        IdGetter = idGetter;
        ExtractIndexedFields = extractIndexedFields;
    }
}
