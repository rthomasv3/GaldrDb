using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query.Planning;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query.Execution;

internal sealed class DynamicQueryExecutor : IDynamicQueryExecutor
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly string _collectionName;
    private readonly CollectionEntry _collection;
    private readonly DynamicDocumentReader _reader;
    private readonly VersionScanner _versionScanner;
    private readonly WriteSetOverlay<JsonDocument> _writeSetOverlay;
    private readonly TransactionContext _context;

    public DynamicQueryExecutor(
        Transaction transaction,
        GaldrDb db,
        VersionIndex versionIndex,
        TxId snapshotTxId,
        string collectionName,
        CollectionEntry collection)
    {
        _transaction = transaction;
        _db = db;
        _collectionName = collectionName;
        _collection = collection;
        _context = transaction.Context;
        _reader = new DynamicDocumentReader();
        SecondaryIndexScanner indexScanner = new SecondaryIndexScanner(db, collectionName, _context);
        _versionScanner = new VersionScanner(db, versionIndex, snapshotTxId, indexScanner, _context);
        _writeSetOverlay = new WriteSetOverlay<JsonDocument>(transaction, collectionName, _reader);
    }

    public List<JsonDocument> ExecuteQuery(DynamicQueryBuilder query)
    {
        QueryExecutionPlan plan = CreateExecutionPlan(query.Filters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<JsonDocument> scanResult = ScanDocuments(plan, skip, limit);
        List<JsonDocument> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, query.Filters);

        List<JsonDocument> result;
        if (plan.RequiresPostScanOrdering)
        {
            List<JsonDocument> sorted = QueryResultProcessor.ApplyDynamicOrdering(merged, query.OrderByClauses);
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

    public async Task<List<JsonDocument>> ExecuteQueryAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        QueryExecutionPlan plan = CreateExecutionPlan(query.Filters, query.OrderByClauses);

        int? skip = plan.CanApplySkipLimitDuringScan ? query.SkipValue : null;
        int? limit = plan.CanApplySkipLimitDuringScan ? query.LimitValue : null;

        QueryScanResult<JsonDocument> scanResult = await ScanDocumentsAsync(plan, skip, limit, cancellationToken).ConfigureAwait(false);
        List<JsonDocument> merged = _writeSetOverlay.Apply(scanResult.Documents, scanResult.DocumentIds, query.Filters);

        List<JsonDocument> result;
        if (plan.RequiresPostScanOrdering)
        {
            List<JsonDocument> sorted = QueryResultProcessor.ApplyDynamicOrdering(merged, query.OrderByClauses);
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

    public int ExecuteCount(DynamicQueryBuilder query)
    {
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(_collectionName, _collection, _transaction);
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(query.Filters);
            CountScanResult countResult = CountDocuments(plan);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, query.Filters);
            count = countResult.Count + adjustment;
        }

        return count;
    }

    public async Task<int> ExecuteCountAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        int count;

        if (query.Filters.Count == 0)
        {
            count = _versionScanner.GetUnfilteredCount(_collectionName, _collection, _transaction);
        }
        else
        {
            QueryExecutionPlan plan = CreateExecutionPlanForCount(query.Filters);
            CountScanResult countResult = await CountDocumentsAsync(plan, cancellationToken).ConfigureAwait(false);
            int adjustment = _writeSetOverlay.GetCountAdjustment(countResult.DocumentIds, query.Filters);
            count = countResult.Count + adjustment;
        }

        return count;
    }

    public bool ExecuteAny(DynamicQueryBuilder query)
    {
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(_collectionName, _collection, _transaction);
        }
        else
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(query.Filters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(query.Filters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = HasAnyDocument(plan, deletedDocIds);
            }
        }

        return found;
    }

    public async Task<bool> ExecuteAnyAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        bool found;

        if (query.Filters.Count == 0)
        {
            found = _versionScanner.HasAnyUnfiltered(_collectionName, _collection, _transaction);
        }
        else
        {
            found = _writeSetOverlay.HasAnyMatchingWriteSetDocument(query.Filters);
            if (!found)
            {
                QueryExecutionPlan plan = CreateExecutionPlanForCount(query.Filters);
                HashSet<int> deletedDocIds = _writeSetOverlay.GetDeletedDocIds();
                found = await HasAnyDocumentAsync(plan, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
        }

        return found;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        QueryExecutionPlan plan = CreateExecutionPlanForCount(filters);
        return QueryExplanation.FromExecutionPlan(plan, filters.Count);
    }

    private QueryExecutionPlan CreateExecutionPlan(IReadOnlyList<IFieldFilter> filters, IReadOnlyList<DynamicOrderByClause> orderByClauses)
    {
        QueryExecutionPlan plan;

        if (_collection != null)
        {
            List<string> orderByFieldNames = new List<string>(orderByClauses.Count);
            foreach (DynamicOrderByClause clause in orderByClauses)
            {
                orderByFieldNames.Add(clause.FieldName);
            }
            bool hasDescendingOrder = orderByClauses.Count > 0 && orderByClauses[0].Descending;

            QueryPlanner planner = new QueryPlanner(_collection);
            plan = planner.CreateExecutionPlan(filters, orderByFieldNames, hasDescendingOrder);
        }
        else
        {
            plan = QueryExecutionPlan.CreateFullScan(filters, orderByClauses.Count > 0);
        }

        return plan;
    }

    private QueryExecutionPlan CreateExecutionPlanForCount(IReadOnlyList<IFieldFilter> filters)
    {
        QueryExecutionPlan plan;

        if (_collection != null)
        {
            QueryPlanner planner = new QueryPlanner(_collection);
            plan = planner.CreateExecutionPlan(filters, new List<string>(), false);
        }
        else
        {
            plan = QueryExecutionPlan.CreateFullScan(filters, false);
        }

        return plan;
    }

    private QueryScanResult<JsonDocument> ScanDocuments(QueryExecutionPlan plan, int? skip, int? limit)
    {
        List<DocumentVersion> versions = _versionScanner.GetVersionsForPlan(_collectionName, plan);
        return MaterializeDocuments(versions, plan.RemainingFilters, skip, limit);
    }

    private async Task<QueryScanResult<JsonDocument>> ScanDocumentsAsync(
        QueryExecutionPlan plan,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<DocumentVersion> versions = await _versionScanner.GetVersionsForPlanAsync(_collectionName, plan, cancellationToken).ConfigureAwait(false);
        return await MaterializeDocumentsAsync(versions, plan.RemainingFilters, skip, limit, cancellationToken).ConfigureAwait(false);
    }

    private QueryScanResult<JsonDocument> MaterializeDocuments(
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location, _context);
            JsonDocument document = _reader.ReadDocument(jsonBytes);

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

        return new QueryScanResult<JsonDocument>(results, docIds);
    }

    private async Task<QueryScanResult<JsonDocument>> MaterializeDocumentsAsync(
        List<DocumentVersion> versions,
        IReadOnlyList<IFieldFilter> filters,
        int? skip,
        int? limit,
        CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;
        bool hasFilters = filters.Count > 0;

        foreach (DocumentVersion version in versions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, _context, cancellationToken).ConfigureAwait(false);
            JsonDocument document = _reader.ReadDocument(jsonBytes);

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

        return new QueryScanResult<JsonDocument>(results, docIds);
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
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location, _context);
                JsonDocument document = _reader.ReadDocument(jsonBytes);

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
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, _context, cancellationToken).ConfigureAwait(false);
                JsonDocument document = _reader.ReadDocument(jsonBytes);

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

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location, _context);
            JsonDocument document = _reader.ReadDocument(jsonBytes);

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

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, _context, cancellationToken).ConfigureAwait(false);
            JsonDocument document = _reader.ReadDocument(jsonBytes);

            if (_reader.PassesFilters(document, plan.RemainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }
}
