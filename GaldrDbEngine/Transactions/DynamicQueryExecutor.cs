using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Transactions;

internal sealed class DynamicQueryExecutor : IDynamicQueryExecutor
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly VersionIndex _versionIndex;
    private readonly TxId _snapshotTxId;
    private readonly string _collectionName;
    private readonly CollectionEntry _collection;

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
        _versionIndex = versionIndex;
        _snapshotTxId = snapshotTxId;
        _collectionName = collectionName;
        _collection = collection;
    }

    public List<JsonDocument> ExecuteQuery(DynamicQueryBuilder query)
    {
        List<JsonDocument> result;
        QueryPlan plan = CreateQueryPlan(query.Filters);
        List<JsonDocument> snapshotResults;
        HashSet<int> snapshotDocIds;

        bool isOrderByIdOnly = IsOrderByIdOnly(query.OrderByClauses);
        bool canUseOptimizedScan = plan.PlanType == QueryPlanType.PrimaryKeyScan && isOrderByIdOnly;
        bool canUseOptimizedRangeScan = plan.PlanType == QueryPlanType.PrimaryKeyRange && isOrderByIdOnly;

        if (canUseOptimizedScan)
        {
            bool descending = query.OrderByClauses.Count > 0 && query.OrderByClauses[0].Descending;
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyScan(query.SkipValue, query.LimitValue, descending);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else if (canUseOptimizedRangeScan)
        {
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(plan, query.Filters, query.SkipValue, query.LimitValue);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else
        {
            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(plan, query.Filters, null, null);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (snapshotResults, snapshotDocIds) = ExecuteSecondaryIndexScan(plan, query.Filters);
            }
            else
            {
                (snapshotResults, snapshotDocIds) = ExecuteFullScan(query.Filters);
            }

            List<JsonDocument> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
            List<JsonDocument> sorted = ApplyOrdering(merged, query.OrderByClauses);
            result = ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }

        return result;
    }

    private static bool IsOrderByIdOnly(IReadOnlyList<DynamicOrderByClause> orderByClauses)
    {
        return orderByClauses.Count == 0 ||
               (orderByClauses.Count == 1 && orderByClauses[0].FieldName == "Id");
    }

    public int ExecuteCount(DynamicQueryBuilder query)
    {
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount();
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(query.Filters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = CountPrimaryKeyRangeScan(plan, query.Filters);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (count, countedDocIds) = CountSecondaryIndexScan(plan, query.Filters);
            }
            else
            {
                (count, countedDocIds) = CountFullScan(query.Filters);
            }

            int adjustment = GetWriteSetCountAdjustment(countedDocIds, query.Filters);
            count += adjustment;
        }

        return count;
    }

    public async Task<List<JsonDocument>> ExecuteQueryAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        List<JsonDocument> result;
        QueryPlan plan = CreateQueryPlan(query.Filters);
        List<JsonDocument> snapshotResults;
        HashSet<int> snapshotDocIds;

        bool isOrderByIdOnly = IsOrderByIdOnly(query.OrderByClauses);
        bool canUseOptimizedScan = plan.PlanType == QueryPlanType.PrimaryKeyScan && isOrderByIdOnly;
        bool canUseOptimizedRangeScan = plan.PlanType == QueryPlanType.PrimaryKeyRange && isOrderByIdOnly;

        if (canUseOptimizedScan)
        {
            bool descending = query.OrderByClauses.Count > 0 && query.OrderByClauses[0].Descending;
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyScanAsync(query.SkipValue, query.LimitValue, descending, cancellationToken).ConfigureAwait(false);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else if (canUseOptimizedRangeScan)
        {
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(plan, query.Filters, query.SkipValue, query.LimitValue, cancellationToken).ConfigureAwait(false);
            result = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        }
        else
        {
            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(plan, query.Filters, null, null, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (snapshotResults, snapshotDocIds) = await ExecuteSecondaryIndexScanAsync(plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (snapshotResults, snapshotDocIds) = await ExecuteFullScanAsync(query.Filters, cancellationToken).ConfigureAwait(false);
            }

            List<JsonDocument> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
            List<JsonDocument> sorted = ApplyOrdering(merged, query.OrderByClauses);
            result = ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
        }

        return result;
    }

    public async Task<int> ExecuteCountAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        int count;

        if (query.Filters.Count == 0)
        {
            count = GetUnfilteredCount();
        }
        else
        {
            QueryPlan plan = CreateQueryPlan(query.Filters);
            HashSet<int> countedDocIds;

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                (count, countedDocIds) = await CountPrimaryKeyRangeScanAsync(plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                (count, countedDocIds) = await CountSecondaryIndexScanAsync(plan, query.Filters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                (count, countedDocIds) = await CountFullScanAsync(query.Filters, cancellationToken).ConfigureAwait(false);
            }

            int adjustment = GetWriteSetCountAdjustment(countedDocIds, query.Filters);
            count += adjustment;
        }

        return count;
    }

    public bool ExecuteAny(DynamicQueryBuilder query)
    {
        bool found;

        if (query.Filters.Count == 0)
        {
            found = HasAnyUnfiltered();
        }
        else
        {
            found = HasAnyFiltered(query.Filters);
        }

        return found;
    }

    public async Task<bool> ExecuteAnyAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        bool found;

        if (query.Filters.Count == 0)
        {
            found = HasAnyUnfiltered();
        }
        else
        {
            found = await HasAnyFilteredAsync(query.Filters, cancellationToken).ConfigureAwait(false);
        }

        return found;
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        QueryPlan plan = CreateQueryPlan(filters);
        return QueryExplanation.FromPlan(plan, filters.Count);
    }

    private QueryPlan CreateQueryPlan(IReadOnlyList<IFieldFilter> filters)
    {
        QueryPlan plan;

        if (_collection != null)
        {
            QueryPlanner planner = new QueryPlanner(_collection);
            plan = planner.CreatePlan(filters);
        }
        else
        {
            plan = QueryPlan.FullScan();
        }

        return plan;
    }

    private (List<JsonDocument> Results, HashSet<int> DocIds) ExecutePrimaryKeyRangeScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, int? skip, int? limit)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

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
                JsonDocument document = JsonDocument.Parse(jsonBytes);
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
                collected++;
            }
            else
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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
                        _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
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

    private async Task<(List<JsonDocument> Results, HashSet<int> DocIds)> ExecutePrimaryKeyRangeScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, int? skip, int? limit, CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

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
                JsonDocument document = JsonDocument.Parse(jsonBytes);
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
                collected++;
            }
            else
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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
                        _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
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

    private (List<JsonDocument> Results, HashSet<int> DocIds) ExecuteSecondaryIndexScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<JsonDocument> Results, HashSet<int> DocIds)> ExecuteSecondaryIndexScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
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
            DynamicInFilter inFilter = (DynamicInFilter)filter;
            foreach (byte[] valueKeyBytes in inFilter.GetAllIndexKeyBytes())
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

    private (List<JsonDocument> Results, HashSet<int> DocIds) ExecutePrimaryKeyScan(int? skip, int? limit, bool descending)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<int> documentIds = _versionIndex.GetDocumentIds(_collectionName);
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
            DocumentVersion version = _versionIndex.GetVisibleVersion(_collectionName, docId, _snapshotTxId);
            if (version != null)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                    JsonDocument document = JsonDocument.Parse(jsonBytes);
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

        return (results, docIds);
    }

    private async Task<(List<JsonDocument> Results, HashSet<int> DocIds)> ExecutePrimaryKeyScanAsync(int? skip, int? limit, bool descending, CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<int> documentIds = _versionIndex.GetDocumentIds(_collectionName);
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
            DocumentVersion version = _versionIndex.GetVisibleVersion(_collectionName, docId, _snapshotTxId);
            if (version != null)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else if (collected < limitCount)
                {
                    byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                    JsonDocument document = JsonDocument.Parse(jsonBytes);
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

        return (results, docIds);
    }

    private (List<JsonDocument> Results, HashSet<int> DocIds) ExecuteFullScan(IReadOnlyList<IFieldFilter> filters)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<JsonDocument> Results, HashSet<int> DocIds)> ExecuteFullScanAsync(IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
                _transaction.RecordRead(_collectionName, version.DocumentId, version.CreatedBy);
            }
        }

        return (results, docIds);
    }

    private static IReadOnlyList<IFieldFilter> GetRemainingFilters(IReadOnlyList<IFieldFilter> filters, int? usedFilterIndex)
    {
        IReadOnlyList<IFieldFilter> result;

        if (!usedFilterIndex.HasValue)
        {
            result = filters;
        }
        else if (filters.Count <= 1)
        {
            result = Array.Empty<IFieldFilter>();
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

    private List<JsonDocument> ApplyWriteSetOverlay(List<JsonDocument> snapshotResults, HashSet<int> snapshotDocIds, IReadOnlyList<IFieldFilter> filters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<JsonDocument> documentsToAdd = new List<JsonDocument>();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
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
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    documentsToAdd.Add(document);
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!snapshotDocIds.Contains(docId))
                {
                    JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                    if (PassesFilters(document, filters))
                    {
                        documentsToAdd.Add(document);
                    }
                }
            }
        }

        List<JsonDocument> results = new List<JsonDocument>();

        foreach (JsonDocument document in snapshotResults)
        {
            int docId = document.GetInt32("Id");
            if (!idsToRemove.Contains(docId))
            {
                results.Add(document);
            }
        }

        results.AddRange(documentsToAdd);

        return results;
    }

    private static bool PassesFilters(JsonDocument document, IReadOnlyList<IFieldFilter> filters)
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

    private static List<JsonDocument> ApplyOrdering(List<JsonDocument> documents, IReadOnlyList<DynamicOrderByClause> orderByClauses)
    {
        List<JsonDocument> result;

        if (orderByClauses.Count == 0)
        {
            result = documents;
        }
        else
        {
            result = new List<JsonDocument>(documents);
            result.Sort((a, b) =>
            {
                int cmp = 0;

                foreach (DynamicOrderByClause clause in orderByClauses)
                {
                    cmp = clause.Compare(a, b);
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

    private static List<JsonDocument> ApplySkipAndLimit(List<JsonDocument> documents, int? skip, int? limit)
    {
        int skipCount = skip ?? 0;
        int startIndex = skipCount;
        List<JsonDocument> results;

        if (startIndex >= documents.Count)
        {
            results = new List<JsonDocument>();
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

            results = new List<JsonDocument>();
            for (int i = startIndex; i < endIndex; i++)
            {
                results.Add(documents[i]);
            }
        }

        return results;
    }

    private int GetUnfilteredCount()
    {
        int count = _collection?.DocumentCount ?? 0;

        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName == _collectionName)
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

    private (int Count, HashSet<int> DocIds) CountFullScan(IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                count++;
                docIds.Add(document.GetInt32("Id"));
            }
        }

        return (count, docIds);
    }

    private async Task<(int Count, HashSet<int> DocIds)> CountFullScanAsync(IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                count++;
                docIds.Add(document.GetInt32("Id"));
            }
        }

        return (count, docIds);
    }

    private (int Count, HashSet<int> DocIds) CountPrimaryKeyRangeScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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

    private async Task<(int Count, HashSet<int> DocIds)> CountPrimaryKeyRangeScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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

    private (int Count, HashSet<int> DocIds) CountSecondaryIndexScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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

    private async Task<(int Count, HashSet<int> DocIds)> CountSecondaryIndexScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        int count = 0;
        HashSet<int> docIds = new HashSet<int>();

        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool hasRemainingFilters = remainingFilters.Count > 0;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (hasRemainingFilters)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
                JsonDocument document = JsonDocument.Parse(jsonBytes);

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

    private int GetWriteSetCountAdjustment(HashSet<int> countedDocIds, IReadOnlyList<IFieldFilter> filters)
    {
        int adjustment = 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
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
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
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
                    JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                    if (PassesFilters(document, filters))
                    {
                        adjustment++;
                    }
                }
            }
        }

        return adjustment;
    }

    private bool HasAnyUnfiltered()
    {
        int snapshotCount = _collection?.DocumentCount ?? 0;
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        bool hasAny;

        if (snapshotCount > 0)
        {
            bool allDeleted = true;
            List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);

            foreach (DocumentVersion version in visibleVersions)
            {
                DocumentKey key = new DocumentKey(_collectionName, version.DocumentId);
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
                if (kvp.Key.CollectionName == _collectionName && kvp.Value.Operation == WriteOperation.Insert)
                {
                    hasAny = true;
                    break;
                }
            }
        }

        return hasAny;
    }

    private bool HasAnyFiltered(IReadOnlyList<IFieldFilter> filters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        HashSet<int> deletedDocIds = new HashSet<int>();
        bool found = false;

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
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
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
        }

        if (!found)
        {
            QueryPlan plan = CreateQueryPlan(filters);

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                found = AnyPrimaryKeyRangeScan(plan, filters, deletedDocIds);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                found = AnySecondaryIndexScan(plan, filters, deletedDocIds);
            }
            else
            {
                found = AnyFullScan(filters, deletedDocIds);
            }
        }

        return found;
    }

    private async Task<bool> HasAnyFilteredAsync(IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        HashSet<int> deletedDocIds = new HashSet<int>();
        bool found = false;

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
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
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                JsonDocument document = JsonDocument.Parse(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    found = true;
                }
            }
        }

        if (!found)
        {
            QueryPlan plan = CreateQueryPlan(filters);

            if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
            {
                found = await AnyPrimaryKeyRangeScanAsync(plan, filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
            else if (plan.PlanType == QueryPlanType.SecondaryIndexScan)
            {
                found = await AnySecondaryIndexScanAsync(plan, filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                found = await AnyFullScanAsync(filters, deletedDocIds, cancellationToken).ConfigureAwait(false);
            }
        }

        return found;
    }

    private bool AnySecondaryIndexScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnySecondaryIndexScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        List<SecondaryIndexEntry> entries = GetSecondaryIndexEntries(plan);
        List<int> indexDocIds = ExtractDocIdsFromEntries(entries);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, indexDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private bool AnyFullScan(IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnyFullScanAsync(IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(_collectionName, _snapshotTxId);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, filters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private bool AnyPrimaryKeyRangeScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds)
    {
        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }

    private async Task<bool> AnyPrimaryKeyRangeScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, HashSet<int> deletedDocIds, CancellationToken cancellationToken)
    {
        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);
        bool found = false;

        foreach (DocumentVersion version in visibleVersions)
        {
            if (deletedDocIds.Contains(version.DocumentId))
            {
                continue;
            }

            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
            {
                found = true;
                break;
            }
        }

        return found;
    }
}
