using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

public sealed class ProjectionQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly VersionIndex _versionIndex;
    private readonly TxId _snapshotTxId;
    private readonly IGaldrProjectionTypeInfo _projTypeInfo;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly Type _sourceType;
    private readonly Type _projectionType;

    public ProjectionQueryExecutor(
        Transaction transaction,
        GaldrDb db,
        VersionIndex versionIndex,
        TxId snapshotTxId,
        IGaldrProjectionTypeInfo projTypeInfo,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _transaction = transaction;
        _db = db;
        _versionIndex = versionIndex;
        _snapshotTxId = snapshotTxId;
        _projTypeInfo = projTypeInfo;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
        _sourceType = projTypeInfo.SourceType;
        _projectionType = typeof(T);
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(query.Filters);

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        List<object> sourceDocuments = new List<object>();
        HashSet<int> snapshotDocIds = new HashSet<int>();

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, sourceFilters))
            {
                sourceDocuments.Add(sourceDoc);
                snapshotDocIds.Add(_projTypeInfo.GetSourceId(sourceDoc));
            }
        }

        List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);

        List<T> projections = ConvertToProjections(merged);

        List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);

        List<T> sorted = ApplyOrdering(filteredProjections, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        return ExecuteQuery(query).Count;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(query.Filters);

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        List<object> sourceDocuments = new List<object>();
        HashSet<int> snapshotDocIds = new HashSet<int>();

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, sourceFilters))
            {
                sourceDocuments.Add(sourceDoc);
                snapshotDocIds.Add(_projTypeInfo.GetSourceId(sourceDoc));
            }
        }

        List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);

        List<T> projections = ConvertToProjections(merged);

        List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);

        List<T> sorted = ApplyOrdering(filteredProjections, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return (await ExecuteQueryAsync(query, cancellationToken).ConfigureAwait(false)).Count;
    }

    private List<object> ApplyWriteSetOverlay(List<object> snapshotResults, HashSet<int> snapshotDocIds, List<IFieldFilter> filters)
    {
        string collectionName = _projTypeInfo.CollectionName;
        IReadOnlyDictionary<(string CollectionName, int DocId), WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<object> documentsToAdd = new List<object>();

        foreach (KeyValuePair<(string CollectionName, int DocId), WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != collectionName)
            {
                continue;
            }

            int docId = kvp.Key.DocId;
            WriteSetEntry entry = kvp.Value;

            if (entry.Operation == WriteOperation.Delete)
            {
                idsToRemove.Add(docId);
            }
            else if (entry.Operation == WriteOperation.Update)
            {
                idsToRemove.Add(docId);
                object sourceDoc = DeserializeSourceDocument(entry.SerializedData);
                if (PassesFilters(sourceDoc, filters))
                {
                    documentsToAdd.Add(sourceDoc);
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!snapshotDocIds.Contains(docId))
                {
                    object sourceDoc = DeserializeSourceDocument(entry.SerializedData);
                    if (PassesFilters(sourceDoc, filters))
                    {
                        documentsToAdd.Add(sourceDoc);
                    }
                }
            }
        }

        List<object> results = new List<object>();

        foreach (object sourceDoc in snapshotResults)
        {
            int docId = _projTypeInfo.GetSourceId(sourceDoc);
            if (!idsToRemove.Contains(docId))
            {
                results.Add(sourceDoc);
            }
        }

        results.AddRange(documentsToAdd);

        return results;
    }

    private object DeserializeSourceDocument(byte[] serializedData)
    {
        string json = Encoding.UTF8.GetString(serializedData);
        return _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);
    }

    private List<T> ConvertToProjections(List<object> sourceDocuments)
    {
        List<T> projections = new List<T>(sourceDocuments.Count);

        foreach (object sourceDoc in sourceDocuments)
        {
            T projection = (T)_projTypeInfo.ConvertToProjection(sourceDoc);
            projections.Add(projection);
        }

        return projections;
    }

    private (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) SeparateFilters(IReadOnlyList<IFieldFilter> filters)
    {
        List<IFieldFilter> sourceFilters = new List<IFieldFilter>();
        List<IFieldFilter> projectionFilters = new List<IFieldFilter>();

        foreach (IFieldFilter filter in filters)
        {
            if (filter.DocumentType == _sourceType)
            {
                sourceFilters.Add(filter);
            }
            else if (filter.DocumentType == _projectionType)
            {
                projectionFilters.Add(filter);
            }
            else
            {
                // If DocumentType matches neither, assume it's a source filter
                sourceFilters.Add(filter);
            }
        }

        return (sourceFilters, projectionFilters);
    }

    private bool PassesFilters(object document, List<IFieldFilter> filters)
    {
        bool passes = true;

        foreach (IFieldFilter filter in filters)
        {
            if (!filter.Evaluate(document))
            {
                passes = false;
                break;
            }
        }

        return passes;
    }

    private List<T> ApplyProjectionFilters(List<T> projections, List<IFieldFilter> filters)
    {
        if (filters.Count == 0)
        {
            return projections;
        }

        List<T> filtered = new List<T>();

        foreach (T projection in projections)
        {
            bool passes = true;
            foreach (IFieldFilter filter in filters)
            {
                if (!filter.Evaluate(projection))
                {
                    passes = false;
                    break;
                }
            }

            if (passes)
            {
                filtered.Add(projection);
            }
        }

        return filtered;
    }

    private List<T> ApplyOrdering(List<T> documents, IReadOnlyList<OrderByClause<T>> orderByClauses)
    {
        if (orderByClauses.Count == 0)
        {
            return documents;
        }

        List<T> sorted = new List<T>(documents);
        sorted.Sort((a, b) =>
        {
            int result = 0;

            foreach (OrderByClause<T> clause in orderByClauses)
            {
                result = clause.Comparer(a, b);
                if (result != 0)
                {
                    break;
                }
            }

            return result;
        });

        return sorted;
    }

    private List<T> ApplySkipAndLimit(List<T> documents, int? skip, int? limit)
    {
        int skipCount = skip ?? 0;
        int startIndex = skipCount;

        if (startIndex >= documents.Count)
        {
            return new List<T>();
        }

        int takeCount;
        if (limit.HasValue)
        {
            takeCount = limit.Value;
        }
        else
        {
            takeCount = documents.Count - startIndex;
        }

        int endIndex = startIndex + takeCount;
        if (endIndex > documents.Count)
        {
            endIndex = documents.Count;
        }

        List<T> results = new List<T>();
        for (int i = startIndex; i < endIndex; i++)
        {
            results.Add(documents[i]);
        }

        return results;
    }
}
