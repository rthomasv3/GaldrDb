using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

internal sealed class ProjectionQueryExecutor<T> : IQueryExecutor<T>
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

        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryPlan plan = CreateQueryPlan(collection, sourceFilters);

        List<object> sourceDocuments;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (sourceDocuments, snapshotDocIds) = ExecutePrimaryKeyRangeScan(collectionName, plan, sourceFilters);
        }
        else
        {
            (sourceDocuments, snapshotDocIds) = ExecuteFullScan(collectionName, sourceFilters);
        }

        List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);

        List<T> projections = ConvertToProjections(merged);

        List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);

        List<T> sorted = ApplyOrdering(filteredProjections, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount(collectionName, collection);
        }
        else if (projectionFilters.Count == 0)
        {
            QueryPlan plan = CreateQueryPlan(collection, sourceFilters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = CountPrimaryKeyRangeScan(collectionName, plan, sourceFilters);
            }
            else
            {
                (count, countedDocIds) = CountFullScan(collectionName, sourceFilters);
            }

            int adjustment = GetWriteSetCountAdjustment(collectionName, countedDocIds, sourceFilters);
            count += adjustment;
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(collection, sourceFilters);
            List<object> sourceDocuments;
            HashSet<int> snapshotDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (sourceDocuments, snapshotDocIds) = ExecutePrimaryKeyRangeScan(collectionName, plan, sourceFilters);
            }
            else
            {
                (sourceDocuments, snapshotDocIds) = ExecuteFullScan(collectionName, sourceFilters);
            }

            List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);
            count = filteredProjections.Count;
        }

        return count;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(query.Filters);

        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryPlan plan = CreateQueryPlan(collection, sourceFilters);

        List<object> sourceDocuments;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (sourceDocuments, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(collectionName, plan, sourceFilters, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            (sourceDocuments, snapshotDocIds) = await ExecuteFullScanAsync(collectionName, sourceFilters, cancellationToken).ConfigureAwait(false);
        }

        List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);

        List<T> projections = ConvertToProjections(merged);

        List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);

        List<T> sorted = ApplyOrdering(filteredProjections, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount(collectionName, collection);
        }
        else if (projectionFilters.Count == 0)
        {
            QueryPlan plan = CreateQueryPlan(collection, sourceFilters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = await CountPrimaryKeyRangeScanAsync(collectionName, plan, sourceFilters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (count, countedDocIds) = await CountFullScanAsync(collectionName, sourceFilters, cancellationToken).ConfigureAwait(false);
            }

            int adjustment = GetWriteSetCountAdjustment(collectionName, countedDocIds, sourceFilters);
            count += adjustment;
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(collection, sourceFilters);
            List<object> sourceDocuments;
            HashSet<int> snapshotDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (sourceDocuments, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(collectionName, plan, sourceFilters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (sourceDocuments, snapshotDocIds) = await ExecuteFullScanAsync(collectionName, sourceFilters, cancellationToken).ConfigureAwait(false);
            }

            List<object> merged = ApplyWriteSetOverlay(sourceDocuments, snapshotDocIds, sourceFilters);
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, projectionFilters);
            count = filteredProjections.Count;
        }

        return count;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        string collectionName = _projTypeInfo.CollectionName;
        (List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters) = SeparateFilters(filters);

        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryPlan plan = CreateQueryPlan(collection, sourceFilters);

        return QueryExplanation.FromPlan(plan, filters.Count);
    }

    private QueryPlan CreateQueryPlan(CollectionEntry collection, List<IFieldFilter> filters)
    {
        QueryPlan plan;

        if (collection != null)
        {
            QueryPlanner planner = new QueryPlanner(collection);
            plan = planner.CreatePlan(filters);
        }
        else
        {
            plan = QueryPlan.FullScan();
        }

        return plan;
    }

    private (List<object> Results, HashSet<int> DocIds) ExecutePrimaryKeyRangeScan(string collectionName, QueryPlan plan, List<IFieldFilter> filters)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        List<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, remainingFilters))
            {
                results.Add(sourceDoc);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<object> Results, HashSet<int> DocIds)> ExecutePrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, List<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        List<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, remainingFilters))
            {
                results.Add(sourceDoc);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private (List<object> Results, HashSet<int> DocIds) ExecuteFullScan(string collectionName, List<IFieldFilter> filters)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, filters))
            {
                results.Add(sourceDoc);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<object> Results, HashSet<int> DocIds)> ExecuteFullScanAsync(string collectionName, List<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, filters))
            {
                results.Add(sourceDoc);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private List<IFieldFilter> GetRemainingFilters(List<IFieldFilter> filters, int? usedFilterIndex)
    {
        List<IFieldFilter> result;

        if (!usedFilterIndex.HasValue)
        {
            result = filters;
        }
        else if (filters.Count <= 1)
        {
            result = new List<IFieldFilter>();
        }
        else
        {
            result = new List<IFieldFilter>(filters.Count - 1);
            for (int i = 0; i < filters.Count; i++)
            {
                if (i != usedFilterIndex.Value)
                {
                    result.Add(filters[i]);
                }
            }
        }

        return result;
    }

    private List<object> ApplyWriteSetOverlay(List<object> snapshotResults, HashSet<int> snapshotDocIds, List<IFieldFilter> filters)
    {
        string collectionName = _projTypeInfo.CollectionName;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<object> documentsToAdd = new List<object>();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
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
        List<T> result;

        if (filters.Count == 0)
        {
            result = projections;
        }
        else
        {
            result = new List<T>();

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
                    result.Add(projection);
                }
            }
        }

        return result;
    }

    private List<T> ApplyOrdering(List<T> documents, IReadOnlyList<OrderByClause<T>> orderByClauses)
    {
        List<T> result;

        if (orderByClauses.Count == 0)
        {
            result = documents;
        }
        else
        {
            result = new List<T>(documents);
            result.Sort((a, b) =>
            {
                int cmp = 0;

                foreach (OrderByClause<T> clause in orderByClauses)
                {
                    cmp = clause.Comparer(a, b);
                    if (cmp != 0)
                    {
                        break;
                    }
                }

                return cmp;
            });
        }

        return result;
    }

    private List<T> ApplySkipAndLimit(List<T> documents, int? skip, int? limit)
    {
        int skipCount = skip ?? 0;
        int startIndex = skipCount;
        List<T> results;

        if (startIndex >= documents.Count)
        {
            results = new List<T>();
        }
        else
        {
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

            results = new List<T>();
            for (int i = startIndex; i < endIndex; i++)
            {
                results.Add(documents[i]);
            }
        }

        return results;
    }

    private int GetUnfilteredCount(string collectionName, CollectionEntry collection)
    {
        int count = collection?.DocumentCount ?? 0;

        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName == collectionName)
            {
                WriteSetEntry entry = kvp.Value;

                if (entry.Operation == WriteOperation.Insert)
                {
                    count++;
                }
                else if (entry.Operation == WriteOperation.Delete)
                {
                    count--;
                }
            }
        }

        return count;
    }

    private (int Count, HashSet<int> DocIds) CountFullScan(string collectionName, List<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, filters))
            {
                count++;
                docIds.Add(_projTypeInfo.GetSourceId(sourceDoc));
            }
        }

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountFullScanAsync(string collectionName, List<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, filters))
            {
                count++;
                docIds.Add(_projTypeInfo.GetSourceId(sourceDoc));
            }
        }

        return (count, docIds);
    }

    private (int Count, HashSet<int> DocIds) CountPrimaryKeyRangeScan(string collectionName, QueryPlan plan, List<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        List<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, remainingFilters))
            {
                count++;
                docIds.Add(version.DocumentId);
            }
        }

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountPrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, List<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        List<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            string json = Encoding.UTF8.GetString(jsonBytes);
            object sourceDoc = _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);

            if (PassesFilters(sourceDoc, remainingFilters))
            {
                count++;
                docIds.Add(version.DocumentId);
            }
        }

        return (count, docIds);
    }

    private int GetWriteSetCountAdjustment(string collectionName, HashSet<int> countedDocIds, List<IFieldFilter> filters)
    {
        int adjustment = 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != collectionName)
            {
                continue;
            }

            int docId = kvp.Key.DocId;
            WriteSetEntry entry = kvp.Value;

            if (entry.Operation == WriteOperation.Delete)
            {
                if (countedDocIds.Contains(docId))
                {
                    adjustment--;
                }
            }
            else if (entry.Operation == WriteOperation.Update)
            {
                object sourceDoc = DeserializeSourceDocument(entry.SerializedData);
                bool nowMatches = PassesFilters(sourceDoc, filters);
                bool wasCountedBefore = countedDocIds.Contains(docId);

                if (wasCountedBefore && !nowMatches)
                {
                    adjustment--;
                }
                else if (!wasCountedBefore && nowMatches)
                {
                    adjustment++;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!countedDocIds.Contains(docId))
                {
                    object sourceDoc = DeserializeSourceDocument(entry.SerializedData);
                    if (PassesFilters(sourceDoc, filters))
                    {
                        adjustment++;
                    }
                }
            }
        }

        return adjustment;
    }
}
