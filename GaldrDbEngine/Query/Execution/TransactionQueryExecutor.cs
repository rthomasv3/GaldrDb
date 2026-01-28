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

internal sealed class TransactionQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly GaldrTypeInfo<T> _typeInfo;
    private readonly TypedDocumentReader<T> _reader;
    private readonly VersionScanner _versionScanner;
    private readonly WriteSetOverlay<T> _writeSetOverlay;

    public TransactionQueryExecutor(
        Transaction transaction,
        GaldrDb db,
        VersionIndex versionIndex,
        TxId snapshotTxId,
        GaldrTypeInfo<T> typeInfo,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _transaction = transaction;
        _db = db;
        _typeInfo = typeInfo;
        _reader = new TypedDocumentReader<T>(typeInfo, jsonSerializer, jsonOptions);
        SecondaryIndexScanner indexScanner = new SecondaryIndexScanner(db);
        _versionScanner = new VersionScanner(db, versionIndex, snapshotTxId, indexScanner);
        _writeSetOverlay = new WriteSetOverlay<T>(transaction, typeInfo.CollectionName, _reader);
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<T> scanResult = ScanDocuments(collectionName, plan, skip, limit);
        List<T> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, query.Filters);

        List<T> result;
        if (plan.RequiresPostScanOrdering)
        {
            List<T> sorted = QueryResultProcessor.ApplyOrdering(merged, query.OrderByClauses);
            result = QueryResultProcessor.ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }
        else if (!plan.CanApplySkipLimitDuringScan)
        {
            result = QueryResultProcessor.ApplySkipAndLimit(merged, query.SkipValue, query.LimitValue);
        }
        else
        {
            result = merged;
        }

        return result;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<T> scanResult = await ScanDocumentsAsync(collectionName, plan, skip, limit, cancellationToken).ConfigureAwait(false);
        List<T> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, query.Filters);

        List<T> result;
        if (plan.RequiresPostScanOrdering)
        {
            List<T> sorted = QueryResultProcessor.ApplyOrdering(merged, query.OrderByClauses);
            result = QueryResultProcessor.ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }
        else if (!plan.CanApplySkipLimitDuringScan)
        {
            result = QueryResultProcessor.ApplySkipAndLimit(merged, query.SkipValue, query.LimitValue);
        }
        else
        {
            result = merged;
        }

        return result;
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(collectionName, collection, _transaction);
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, Array.Empty<OrderByClause<T>>());
            CountScanResult countResult = CountDocuments(collectionName, plan);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, query.Filters);
            count = countResult.Count + adjustment;
        }

        return count;
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(collectionName, collection, _transaction);
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, Array.Empty<OrderByClause<T>>());
            CountScanResult countResult = await CountDocumentsAsync(collectionName, plan, cancellationToken).ConfigureAwait(false);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, query.Filters);
            count = countResult.Count + adjustment;
        }

        return count;
    }

    public bool ExecuteAny(QueryBuilder<T> query)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(collectionName, collection, _transaction);
        }
        else
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(query.Filters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, Array.Empty<OrderByClause<T>>());
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = HasAnyDocument(collectionName, plan, deletedDocIds);
            }
        }

        return found;
    }

    public async Task<bool> ExecuteAnyAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(collectionName, collection, _transaction);
        }
        else
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(query.Filters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlan(collection, query.Filters, Array.Empty<OrderByClause<T>>());
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = await HasAnyDocumentAsync(collectionName, plan, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
        }

        return found;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryExecutionPlan plan = CreateExecutionPlan(collection, filters, Array.Empty<OrderByClause<T>>());

        return QueryExplanation.FromExecutionPlan(plan, filters.Count);
    }

    private QueryExecutionPlan CreateExecutionPlan(
        CollectionEntry collection,
        IReadOnlyList<IFieldFilter> filters,
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

    private QueryScanResult<T> ScanDocuments(
        string collectionName,
        QueryExecutionPlan plan,
        int? skip,
        int? limit)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(collectionName, plan);
        return MaterializeDocuments(collectionName, versions, plan.RemainingFilters, skip, limit);
    }

    private async Task<QueryScanResult<T>> ScanDocumentsAsync(
        string collectionName,
        QueryExecutionPlan plan,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(collectionName, plan, cancellationToken).ConfigureAwait(false);
        return await MaterializeDocumentsAsync(collectionName, versions, plan.RemainingFilters, skip, limit, cancellationToken).ConfigureAwait(false);
    }

    private QueryScanResult<T> MaterializeDocuments(
        string collectionName,
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _reader.ReadDocument(jsonBytes);

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
                    _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
                    collected++;
                }
                else
                {
                    break;
                }
            }
        }

        return new QueryScanResult<T>(results, docIds);
    }

    private async Task<QueryScanResult<T>> MaterializeDocumentsAsync(
        string collectionName,
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _reader.ReadDocument(jsonBytes);

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
                    _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
                    collected++;
                }
                else
                {
                    break;
                }
            }
        }

        return new QueryScanResult<T>(results, docIds);
    }

    private CountScanResult CountDocuments(string collectionName, QueryExecutionPlan plan)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(collectionName, plan);
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();
        bool hasFilters = plan.RemainingFilters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            if (hasFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                T document = _reader.ReadDocument(jsonBytes);

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

    private async Task<CountScanResult> CountDocumentsAsync(
        string collectionName,
        QueryExecutionPlan plan,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(collectionName, plan, cancellationToken).ConfigureAwait(false);
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();
        bool hasFilters = plan.RemainingFilters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            if (hasFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                T document = _reader.ReadDocument(jsonBytes);

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

    private bool HasAnyDocument(string collectionName, QueryExecutionPlan plan, HashSet<int> deletedDocIds)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(collectionName, plan);
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
            T document = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(document, plan.RemainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> HasAnyDocumentAsync(
        string collectionName,
        QueryExecutionPlan plan,
        HashSet<int> deletedDocIds,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(collectionName, plan, cancellationToken).ConfigureAwait(false);
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
            T document = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(document, plan.RemainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }
}
