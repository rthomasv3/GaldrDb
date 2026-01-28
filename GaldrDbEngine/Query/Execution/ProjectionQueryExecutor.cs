using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query.Planning;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;
using GaldrJson;

namespace GaldrDbEngine.Query.Execution;

internal sealed class ProjectionQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly string _collectionName;
    private readonly IGaldrProjectionTypeInfo _projTypeInfo;
    private readonly Type _sourceType;
    private readonly Type _projectionType;
    private readonly ProjectionDocumentReader _reader;
    private readonly VersionScanner _versionScanner;
    private readonly WriteSetOverlay<object> _writeSetOverlay;

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
        _collectionName = projTypeInfo.CollectionName;
        _projTypeInfo = projTypeInfo;
        _sourceType = projTypeInfo.SourceType;
        _projectionType = typeof(T);
        _reader = new ProjectionDocumentReader(projTypeInfo, jsonSerializer, jsonOptions);
        SecondaryIndexScanner indexScanner = new SecondaryIndexScanner(db);
        _versionScanner = new VersionScanner(db, versionIndex, snapshotTxId, indexScanner);
        _writeSetOverlay = new WriteSetOverlay<object>(transaction, projTypeInfo.CollectionName, _reader);
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        QueryExecutionPlan plan = CreateExecutionPlan(collection, filterSeparation.SourceFilters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<object> scanResult = ScanDocuments(plan, skip, limit);
        List<object> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, filterSeparation.SourceFilters);

        List<T> result;
        if (filterSeparation.ProjectionFilters.Count > 0 || plan.RequiresPostScanOrdering)
        {
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, filterSeparation.ProjectionFilters);
            List<T> sorted = QueryResultProcessor.ApplyOrdering(filteredProjections, query.OrderByClauses);
            result = QueryResultProcessor.ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }
        else if (!plan.CanApplySkipLimitDuringScan)
        {
            List<T> projections = ConvertToProjections(merged);
            result = QueryResultProcessor.ApplySkipAndLimit(projections, query.SkipValue, query.LimitValue);
        }
        else
        {
            result = ConvertToProjections(merged);
        }

        return result;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        QueryExecutionPlan plan = CreateExecutionPlan(collection, filterSeparation.SourceFilters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<object> scanResult = await ScanDocumentsAsync(plan, skip, limit, cancellationToken).ConfigureAwait(false);
        List<object> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, filterSeparation.SourceFilters);

        List<T> result;
        if (filterSeparation.ProjectionFilters.Count > 0 || plan.RequiresPostScanOrdering)
        {
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, filterSeparation.ProjectionFilters);
            List<T> sorted = QueryResultProcessor.ApplyOrdering(filteredProjections, query.OrderByClauses);
            result = QueryResultProcessor.ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }
        else if (!plan.CanApplySkipLimitDuringScan)
        {
            List<T> projections = ConvertToProjections(merged);
            result = QueryResultProcessor.ApplySkipAndLimit(projections, query.SkipValue, query.LimitValue);
        }
        else
        {
            result = ConvertToProjections(merged);
        }

        return result;
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(_collectionName, collection, _transaction);
        }
        else if (filterSeparation.ProjectionFilters.Count == 0)
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
            CountScanResult countResult = CountDocuments(plan);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, filterSeparation.SourceFilters);
            count = countResult.Count + adjustment;
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
            QueryScanResult<object> scanResult = ScanDocuments(plan, null, null);
            List<object> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, filterSeparation.SourceFilters);
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, filterSeparation.ProjectionFilters);
            count = filteredProjections.Count;
        }

        return count;
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(_collectionName, collection, _transaction);
        }
        else if (filterSeparation.ProjectionFilters.Count == 0)
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
            CountScanResult countResult = await CountDocumentsAsync(plan, cancellationToken).ConfigureAwait(false);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, filterSeparation.SourceFilters);
            count = countResult.Count + adjustment;
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
            QueryScanResult<object> scanResult = await ScanDocumentsAsync(plan, null, null, cancellationToken).ConfigureAwait(false);
            List<object> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, filterSeparation.SourceFilters);
            List<T> projections = ConvertToProjections(merged);
            List<T> filteredProjections = ApplyProjectionFilters(projections, filterSeparation.ProjectionFilters);
            count = filteredProjections.Count;
        }

        return count;
    }

    public bool ExecuteAny(QueryBuilder<T> query)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(_collectionName, collection, _transaction);
        }
        else if (filterSeparation.ProjectionFilters.Count == 0)
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(filterSeparation.SourceFilters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = HasAnyDocument(plan, deletedDocIds);
            }
        }
        else
        {
            found = HasAnyWithProjectionFilters(filterSeparation.SourceFilters, filterSeparation.ProjectionFilters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = HasAnyDocumentWithProjection(plan, deletedDocIds, filterSeparation.SourceFilters, filterSeparation.ProjectionFilters);
            }
        }

        return found;
    }

    public async Task<bool> ExecuteAnyAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(query.Filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(_collectionName, collection, _transaction);
        }
        else if (filterSeparation.ProjectionFilters.Count == 0)
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(filterSeparation.SourceFilters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = await HasAnyDocumentAsync(plan, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            found = HasAnyWithProjectionFilters(filterSeparation.SourceFilters, filterSeparation.ProjectionFilters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = await HasAnyDocumentWithProjectionAsync(plan, deletedDocIds, filterSeparation.SourceFilters, filterSeparation.ProjectionFilters, cancellationToken).ConfigureAwait(false);
            }
        }

        return found;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        FilterSeparationResult filterSeparation = SeparateFilters(filters);
        CollectionEntry collection = _db.GetCollection(_collectionName);
        QueryExecutionPlan plan = CreateExecutionPlanForCount(collection, filterSeparation.SourceFilters);

        return QueryExplanation.FromExecutionPlan(plan, filters.Count);
    }

    private QueryExecutionPlan CreateExecutionPlan(
        CollectionEntry collection,
        List<IFieldFilter> filters,
        IReadOnlyList<OrderByClause<T>> orderByClauses)
    {
        QueryExecutionPlan plan;

        if (collection != null)
        {
            List<string> orderByFieldNames = new List<string>(orderByClauses.Count);
            foreach (OrderByClause<T> clause in orderByClauses)
            {
                orderByFieldNames.Add(clause.FieldName);
            }
            bool hasDescendingOrder = orderByClauses.Count > 0 && orderByClauses[0].Descending;

            QueryPlanner planner = new QueryPlanner(collection);
            plan = planner.CreateExecutionPlan(filters, orderByFieldNames, hasDescendingOrder);
        }
        else
        {
            plan = QueryExecutionPlan.CreateFullScan(filters, orderByClauses.Count > 0);
        }

        return plan;
    }

    private QueryExecutionPlan CreateExecutionPlanForCount(CollectionEntry collection, List<IFieldFilter> filters)
    {
        QueryExecutionPlan plan;

        if (collection != null)
        {
            QueryPlanner planner = new QueryPlanner(collection);
            plan = planner.CreateExecutionPlan(filters, new List<string>(), false);
        }
        else
        {
            plan = QueryExecutionPlan.CreateFullScan(filters, false);
        }

        return plan;
    }

    private QueryScanResult<object> ScanDocuments(QueryExecutionPlan plan, int? skip, int? limit)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(_collectionName, plan);
        return MaterializeDocuments(versions, plan.RemainingFilters, skip, limit);
    }

    private async Task<QueryScanResult<object>> ScanDocumentsAsync(
        QueryExecutionPlan plan,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(_collectionName, plan, cancellationToken).ConfigureAwait(false);
        return await MaterializeDocumentsAsync(versions, plan.RemainingFilters, skip, limit, cancellationToken).ConfigureAwait(false);
    }

    private QueryScanResult<object> MaterializeDocuments(
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            object document = _reader.ReadDocument(jsonBytes);

            if (!hasFilters || _reader.PassesFilters(document, filters))
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    results.Add(document);
                    docIds.Add(version.DocumentId);
                    _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
                    collected++;
                }
                else
                {
                    break;
                }
            }
        }

        return new QueryScanResult<object>(results, docIds);
    }

    private async Task<QueryScanResult<object>> MaterializeDocumentsAsync(
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<object> results = new List<object>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            object document = _reader.ReadDocument(jsonBytes);

            if (!hasFilters || _reader.PassesFilters(document, filters))
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    results.Add(document);
                    docIds.Add(version.DocumentId);
                    _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
                    collected++;
                }
                else
                {
                    break;
                }
            }
        }

        return new QueryScanResult<object>(results, docIds);
    }

    private CountScanResult CountDocuments(QueryExecutionPlan plan)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(_collectionName, plan);
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();
        bool hasFilters = plan.RemainingFilters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            if (hasFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                object document = _reader.ReadDocument(jsonBytes);

                if (_reader.PassesFilters(document, plan.RemainingFilters))
                {
                    count++;
                    docIds.Add(version.DocumentId);
                }
            }
            else
            {
                count++;
                docIds.Add(version.DocumentId);
            }
        }

        return new CountScanResult(count, docIds);
    }

    private async Task<CountScanResult> CountDocumentsAsync(QueryExecutionPlan plan, CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(_collectionName, plan, cancellationToken).ConfigureAwait(false);
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();
        bool hasFilters = plan.RemainingFilters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            if (hasFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                object document = _reader.ReadDocument(jsonBytes);

                if (_reader.PassesFilters(document, plan.RemainingFilters))
                {
                    count++;
                    docIds.Add(version.DocumentId);
                }
            }
            else
            {
                count++;
                docIds.Add(version.DocumentId);
            }
        }

        return new CountScanResult(count, docIds);
    }

    private bool HasAnyDocument(QueryExecutionPlan plan, HashSet<int> deletedDocIds)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(_collectionName, plan);
        bool found = false;

        foreach (DocumentVersion version in versions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            if (plan.RemainingFilters.Count == 0)
            {
                found = true;
                break;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            object document = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(document, plan.RemainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> HasAnyDocumentAsync(
        QueryExecutionPlan plan,
        HashSet<int> deletedDocIds,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(_collectionName, plan, cancellationToken).ConfigureAwait(false);
        bool found = false;

        foreach (DocumentVersion version in versions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            if (plan.RemainingFilters.Count == 0)
            {
                found = true;
                break;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            object document = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(document, plan.RemainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private bool HasAnyWithProjectionFilters(List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        bool found = false;

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
            {
                continue;
            }

            WriteSetEntry entry = kvp.Value;

            if (entry.Operation == WriteOperation.Update || entry.Operation == WriteOperation.Insert)
            {
                object sourceDoc = _reader.ReadDocument(entry.SerializedData);
                if (_reader.PassesFilters(sourceDoc, sourceFilters))
                {
                    T projection = (T)_projTypeInfo.ConvertToProjection(sourceDoc);
                    if (PassesProjectionFilters(projection, projectionFilters))
                    {
                        found = true;
                        break;
                    }
                }
            }
        }

        return found;
    }

    private bool HasAnyDocumentWithProjection(
        QueryExecutionPlan plan,
        HashSet<int> deletedDocIds,
        List<IFieldFilter> sourceFilters,
        List<IFieldFilter> projectionFilters)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(_collectionName, plan);
        bool found = false;

        foreach (DocumentVersion version in versions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            object sourceDoc = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(sourceDoc, plan.RemainingFilters))
            {
                T projection = (T)_projTypeInfo.ConvertToProjection(sourceDoc);
                if (PassesProjectionFilters(projection, projectionFilters))
                {
                    found = true;
                    break;
                }
            }
        }

        return found;
    }

    private async Task<bool> HasAnyDocumentWithProjectionAsync(
        QueryExecutionPlan plan,
        HashSet<int> deletedDocIds,
        List<IFieldFilter> sourceFilters,
        List<IFieldFilter> projectionFilters,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(_collectionName, plan, cancellationToken).ConfigureAwait(false);
        bool found = false;

        foreach (DocumentVersion version in versions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            object sourceDoc = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(sourceDoc, plan.RemainingFilters))
            {
                T projection = (T)_projTypeInfo.ConvertToProjection(sourceDoc);
                if (PassesProjectionFilters(projection, projectionFilters))
                {
                    found = true;
                    break;
                }
            }
        }

        return found;
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

    private FilterSeparationResult SeparateFilters(IReadOnlyList<IFieldFilter> filters)
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

        return new FilterSeparationResult(sourceFilters, projectionFilters);
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
                if (PassesProjectionFilters(projection, filters))
                {
                    result.Add(projection);
                }
            }
        }

        return result;
    }

    private bool PassesProjectionFilters(T projection, List<IFieldFilter> filters)
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

        return passes;
    }
}
