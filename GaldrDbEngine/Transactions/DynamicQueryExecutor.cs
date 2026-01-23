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
        QueryPlan plan = CreateQueryPlan(query.Filters);
        List<JsonDocument> snapshotResults;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(plan, query.Filters);
        }
        else
        {
            (snapshotResults, snapshotDocIds) = ExecuteFullScan(query.Filters);
        }

        List<JsonDocument> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        List<JsonDocument> sorted = ApplyOrdering(merged, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
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
        QueryPlan plan = CreateQueryPlan(query.Filters);
        List<JsonDocument> snapshotResults;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(plan, query.Filters, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            (snapshotResults, snapshotDocIds) = await ExecuteFullScanAsync(query.Filters, cancellationToken).ConfigureAwait(false);
        }

        List<JsonDocument> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        List<JsonDocument> sorted = ApplyOrdering(merged, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
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
            else
            {
                (count, countedDocIds) = await CountFullScanAsync(query.Filters, cancellationToken).ConfigureAwait(false);
            }

            int adjustment = GetWriteSetCountAdjustment(countedDocIds, query.Filters);
            count += adjustment;
        }

        return count;
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

    private (List<JsonDocument> Results, HashSet<int> DocIds) ExecutePrimaryKeyRangeScan(QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

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

    private async Task<(List<JsonDocument> Results, HashSet<int> DocIds)> ExecutePrimaryKeyRangeScanAsync(QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<JsonDocument> results = new List<JsonDocument>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(_collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(_collectionName, rangeDocIds, _snapshotTxId);

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

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
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

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            JsonDocument document = JsonDocument.Parse(jsonBytes);

            if (PassesFilters(document, remainingFilters))
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
}
