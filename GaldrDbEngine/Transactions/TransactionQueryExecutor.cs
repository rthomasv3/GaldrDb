using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

internal sealed class TransactionQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly VersionIndex _versionIndex;
    private readonly TxId _snapshotTxId;
    private readonly GaldrTypeInfo<T> _typeInfo;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;

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
        _versionIndex = versionIndex;
        _snapshotTxId = snapshotTxId;
        _typeInfo = typeInfo;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        List<T> result;
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);

        QueryPlan plan = CreateQueryPlan(collection, query.Filters);
        List<T> snapshotResults;
        HashSet<int> snapshotDocIds;

        bool isOrderByIdOnly = IsOrderByIdOnly(query.OrderByClauses);
        bool canUseOptimizedScan = plan.PlanType == QueryPlanType.PrimaryKeyScan && isOrderByIdOnly;
        bool canUseOptimizedRangeScan = plan.PlanType == QueryPlanType.PrimaryKeyRange && isOrderByIdOnly;

        if (canUseOptimizedScan)
        {
            bool descending = query.OrderByClauses.Count > 0 && query.OrderByClauses[0].Descending;
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyScan(collectionName, query.SkipValue, query.LimitValue, descending);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else if (canUseOptimizedRangeScan)
        {
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(collectionName, plan, query.Filters, query.SkipValue, query.LimitValue);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else
        {
            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(collectionName, plan, query.Filters, null, null);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (snapshotResults, snapshotDocIds) = ExecuteSecondaryIndexScan(collectionName, plan, query.Filters);
            }
            else
            {
                (snapshotResults, snapshotDocIds) = ExecuteFullScan(collectionName, query.Filters);
            }

            List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
            List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);
            result = ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }

        return result;
    }

    private static bool IsOrderByIdOnly(IReadOnlyList<OrderByClause<T>> orderByClauses)
    {
        return orderByClauses.Count == 0 ||
               (orderByClauses.Count == 1 && orderByClauses[0].FieldName == "Id");
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount(collectionName, collection);
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(collection, query.Filters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = CountPrimaryKeyRangeScan(collectionName, plan, query.Filters);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (count, countedDocIds) = CountSecondaryIndexScan(collectionName, plan, query.Filters);
            }
            else
            {
                (count, countedDocIds) = CountFullScan(collectionName, query.Filters);
            }

            int adjustment = GetWriteSetCountAdjustment(collectionName, countedDocIds, query.Filters);
            count += adjustment;
        }

        return count;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        List<T> result;
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);

        QueryPlan plan = CreateQueryPlan(collection, query.Filters);
        List<T> snapshotResults;
        HashSet<int> snapshotDocIds;

        bool isOrderByIdOnly = IsOrderByIdOnly(query.OrderByClauses);
        bool canUseOptimizedScan = plan.PlanType == QueryPlanType.PrimaryKeyScan && isOrderByIdOnly;
        bool canUseOptimizedRangeScan = plan.PlanType == QueryPlanType.PrimaryKeyRange && isOrderByIdOnly;

        if (canUseOptimizedScan)
        {
            bool descending = query.OrderByClauses.Count > 0 && query.OrderByClauses[0].Descending;
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyScanAsync(collectionName, query.SkipValue, query.LimitValue, descending, cancellationToken).ConfigureAwait(false);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else if (canUseOptimizedRangeScan)
        {
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(collectionName, plan, query.Filters, query.SkipValue, query.LimitValue, cancellationToken).ConfigureAwait(false);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else
        {
            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(collectionName, plan, query.Filters, null, null, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (snapshotResults, snapshotDocIds) = await ExecuteSecondaryIndexScanAsync(collectionName, plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (snapshotResults, snapshotDocIds) = await ExecuteFullScanAsync(collectionName, query.Filters, cancellationToken).ConfigureAwait(false);
            }

            List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
            List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);
            result = ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }

        return result;
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount(collectionName, collection);
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(collection, query.Filters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = await CountPrimaryKeyRangeScanAsync(collectionName, plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (count, countedDocIds) = await CountSecondaryIndexScanAsync(collectionName, plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (count, countedDocIds) = await CountFullScanAsync(collectionName, query.Filters, cancellationToken).ConfigureAwait(false);
            }

            int adjustment = GetWriteSetCountAdjustment(collectionName, countedDocIds, query.Filters);
            count += adjustment;
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
            found = HasAnyUnfiltered(collectionName, collection);
        }
        else
        {
            found = HasAnyFiltered(collectionName, collection, query.Filters);
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
            found = HasAnyUnfiltered(collectionName, collection);
        }
        else
        {
            found = await HasAnyFilteredAsync(collectionName, collection, query.Filters, cancellationToken).ConfigureAwait(false);
        }

        return found;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);
        QueryPlan plan = CreateQueryPlan(collection, filters);

        return QueryExplanation.FromPlan(plan, filters.Count);
    }

    private QueryPlan CreateQueryPlan(CollectionEntry collection, IReadOnlyList<IFieldFilter> filters)
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

    private (List<T> Results, HashSet<int> DocIds) ExecutePrimaryKeyRangeScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, int? skip, int? limit)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (!hasRemainingFilters)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                    continue;
                }

                if (collected >= limitCount)
                {
                    break;
                }

                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
                collected++;
            }
            else
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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
        }

        return (results, docIds);
    }

    private async Task<(List<T> Results, HashSet<int> DocIds)> ExecutePrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, int? skip, int? limit, CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (!hasRemainingFilters)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                    continue;
                }

                if (collected >= limitCount)
                {
                    break;
                }

                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
                collected++;
            }
            else
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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
        }

        return (results, docIds);
    }

    private (List<T> Results, HashSet<int> DocIds) ExecuteSecondaryIndexScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<T> Results, HashSet<int> DocIds)> ExecuteSecondaryIndexScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private List<SecondaryIndexEntry> GetSecondaryIndexEntries(QueryPlan plan)
    {
        List<SecondaryIndexEntry> entries;
        IFieldFilter filter = plan.IndexFilter;
        byte[] keyBytes = filter.GetIndexKeyBytes();

        if (filter.Operation == FieldOp.Equals)
        {
            entries = _db.SearchSecondaryIndexExact(plan.IndexDefinition, keyBytes);
        }
        else if (filter.Operation == FieldOp.StartsWith)
        {
            entries = _db.SearchSecondaryIndex(plan.IndexDefinition, keyBytes);
        }
        else if (filter.Operation == FieldOp.In)
        {
            entries = new List<SecondaryIndexEntry>();
            foreach (byte[] valueKeyBytes in filter.GetAllIndexKeyBytes())
            {
                List<SecondaryIndexEntry> valueEntries = _db.SearchSecondaryIndexExact(plan.IndexDefinition, valueKeyBytes);
                entries.AddRange(valueEntries);
            }
        }
        else if (filter.Operation == FieldOp.Between)
        {
            byte[] endKeyBytes = filter.GetIndexKeyEndBytes();
            entries = _db.SearchSecondaryIndexRange(plan.IndexDefinition, keyBytes, endKeyBytes, true, true);
        }
        else if (filter.Operation == FieldOp.GreaterThan)
        {
            entries = _db.SearchSecondaryIndexRange(plan.IndexDefinition, keyBytes, null, false, true);
        }
        else if (filter.Operation == FieldOp.GreaterThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(plan.IndexDefinition, keyBytes, null, true, true);
        }
        else if (filter.Operation == FieldOp.LessThan)
        {
            entries = _db.SearchSecondaryIndexRange(plan.IndexDefinition, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, false);
        }
        else if (filter.Operation == FieldOp.LessThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(plan.IndexDefinition, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, true);
        }
        else
        {
            entries = new List<SecondaryIndexEntry>();
        }

        return entries;
    }

    private static List<int> ExtractDocIdsFromEntries(List<SecondaryIndexEntry> entries)
    {
        List<int> docIds = new List<int>(entries.Count);

        foreach (SecondaryIndexEntry entry in entries)
        {
            if (!docIds.Contains(entry.DocId))
            {
                docIds.Add(entry.DocId);
            }
        }

        return docIds;
    }

    private (List<T> Results, HashSet<int> DocIds) ExecutePrimaryKeyScan(string collectionName, int? skip, int? limit, bool descending)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<int> documentIds = _versionIndex.GetDocumentIds(collectionName);
        if (descending)
        {
            documentIds.Reverse();
        }

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;

        foreach (int docId in documentIds)
        {
            DocumentVersion version = _versionIndex.GetVisibleVersion(collectionName, docId, _snapshotTxId);
            if (version != null)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                    T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);
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

        return (results, docIds);
    }

    private async Task<(List<T> Results, HashSet<int> DocIds)> ExecutePrimaryKeyScanAsync(string collectionName, int? skip, int? limit, bool descending, CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<int> documentIds = _versionIndex.GetDocumentIds(collectionName);
        if (descending)
        {
            documentIds.Reverse();
        }

        int skipCount = skip ?? 0;
        int limitCount = limit ?? int.MaxValue;
        int skipped = 0;
        int collected = 0;

        foreach (int docId in documentIds)
        {
            DocumentVersion version = _versionIndex.GetVisibleVersion(collectionName, docId, _snapshotTxId);
            if (version != null)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                    T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);
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

        return (results, docIds);
    }

    private (List<T> Results, HashSet<int> DocIds) ExecuteFullScan(string collectionName, IReadOnlyList<IFieldFilter> filters)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<T> Results, HashSet<int> DocIds)> ExecuteFullScanAsync(string collectionName, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private IReadOnlyList<IFieldFilter> GetRemainingFilters(IReadOnlyList<IFieldFilter> filters, int? usedFilterIndex)
    {
        IReadOnlyList<IFieldFilter> result;

        if (!usedFilterIndex.HasValue)
        {
            result = filters;
        }
        else if (filters.Count <= 1)
        {
            result = System.Array.Empty<IFieldFilter>();
        }
        else
        {
            List<IFieldFilter> remaining = new List<IFieldFilter>(filters.Count - 1);
            for (int i = 0; i < filters.Count; i++)
            {
                if (i != usedFilterIndex.Value)
                {
                    remaining.Add(filters[i]);
                }
            }
            result = remaining;
        }

        return result;
    }

    private List<T> ApplyWriteSetOverlay(List<T> snapshotResults, HashSet<int> snapshotDocIds, IReadOnlyList<IFieldFilter> filters)
    {
        string collectionName = _typeInfo.CollectionName;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<T> documentsToAdd = new List<T>();

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
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    documentsToAdd.Add(document);
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!snapshotDocIds.Contains(docId))
                {
                    T document = DeserializeDocument(entry.SerializedData);
                    if (PassesFilters(document, filters))
                    {
                        documentsToAdd.Add(document);
                    }
                }
            }
        }

        List<T> results = new List<T>();

        foreach (T document in snapshotResults)
        {
            int docId = _typeInfo.IdGetter(document);
            if (!idsToRemove.Contains(docId))
            {
                results.Add(document);
            }
        }

        results.AddRange(documentsToAdd);

        return results;
    }

    private T DeserializeDocument(byte[] serializedData)
    {
        string json = Encoding.UTF8.GetString(serializedData);
        return _jsonSerializer.Deserialize<T>(json, _jsonOptions);
    }

    private bool PassesFilters(T document, IReadOnlyList<IFieldFilter> filters)
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

    private (int Count, HashSet<int> DocIds) CountFullScan(string collectionName, IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                count++;
                docIds.Add(_typeInfo.IdGetter(document));
            }
        }

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountFullScanAsync(string collectionName, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                count++;
                docIds.Add(_typeInfo.IdGetter(document));
            }
        }

        return (count, docIds);
    }

    private (int Count, HashSet<int> DocIds) CountPrimaryKeyRangeScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountPrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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

        return (count, docIds);
    }

    private (int Count, HashSet<int> DocIds) CountSecondaryIndexScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountSecondaryIndexScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

                if (PassesFilters(document, remainingFilters))
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

        return (count, docIds);
    }

    private int GetWriteSetCountAdjustment(string collectionName, HashSet<int> countedDocIds, IReadOnlyList<IFieldFilter> filters)
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
                T document = DeserializeDocument(entry.SerializedData);
                bool nowMatches = PassesFilters(document, filters);
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
                    T document = DeserializeDocument(entry.SerializedData);
                    if (PassesFilters(document, filters))
                    {
                        adjustment++;
                    }
                }
            }
        }

        return adjustment;
    }

    private bool HasAnyUnfiltered(string collectionName, CollectionEntry collection)
    {
        int snapshotCount = collection?.DocumentCount ?? 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        bool hasAny;

        if (snapshotCount > 0)
        {
            bool allDeleted = true;
            List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

            foreach (DocumentVersion version in visibleVersions)
            {
                DocumentKey key = new DocumentKey(collectionName, version.DocumentId);
                if (!writeSet.TryGetValue(key, out WriteSetEntry entry) || entry.Operation != WriteOperation.Delete)
                {
                    allDeleted = false;
                    break;
                }
            }

            hasAny = !allDeleted;
        }
        else
        {
            hasAny = false;
        }

        if (!hasAny)
        {
            foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
            {
                if (kvp.Key.CollectionName == collectionName && kvp.Value.Operation == WriteOperation.Insert)
                {
                    hasAny = true;
                    break;
                }
            }
        }

        return hasAny;
    }

    private bool HasAnyFiltered(string collectionName, CollectionEntry collection, IReadOnlyList<IFieldFilter> filters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        HashSet<int> deletedDocIds = new HashSet<int>();
        bool found = false;

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != collectionName)
            {
                continue;
            }

            WriteSetEntry entry = kvp.Value;

            if (entry.Operation == WriteOperation.Delete)
            {
                deletedDocIds.Add(kvp.Key.DocId);
            }
            else if (entry.Operation == WriteOperation.Update)
            {
                deletedDocIds.Add(kvp.Key.DocId);
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
        }

        if (!found)
        {
            QueryPlan plan = CreateQueryPlan(collection, filters);

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                found = AnyPrimaryKeyRangeScan(collectionName, plan, filters, deletedDocIds);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                found = AnySecondaryIndexScan(collectionName, plan, filters, deletedDocIds);
            }
            else
            {
                found = AnyFullScan(collectionName, filters, deletedDocIds);
            }
        }

        return found;
    }

    private async Task<bool> HasAnyFilteredAsync(string collectionName, CollectionEntry collection, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        HashSet<int> deletedDocIds = new HashSet<int>();
        bool found = false;

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != collectionName)
            {
                continue;
            }

            WriteSetEntry entry = kvp.Value;

            if (entry.Operation == WriteOperation.Delete)
            {
                deletedDocIds.Add(kvp.Key.DocId);
            }
            else if (entry.Operation == WriteOperation.Update)
            {
                deletedDocIds.Add(kvp.Key.DocId);
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
        }

        if (!found)
        {
            QueryPlan plan = CreateQueryPlan(collection, filters);

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                found = await AnyPrimaryKeyRangeScanAsync(collectionName, plan, filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                found = await AnySecondaryIndexScanAsync(collectionName, plan, filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                found = await AnyFullScanAsync(collectionName, filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
        }

        return found;
    }

    private bool AnyFullScan(string collectionName, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnyFullScanAsync(string collectionName, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, filters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private bool AnyPrimaryKeyRangeScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnyPrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private bool AnySecondaryIndexScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnySecondaryIndexScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }
}
