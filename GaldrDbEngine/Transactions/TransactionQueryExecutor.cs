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
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);

        QueryPlan plan = CreateQueryPlan(collection, query.Filters);
        List<T> snapshotResults;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (snapshotResults, snapshotDocIds) = ExecutePrimaryKeyRangeScan(collectionName, plan, query.Filters);
        }
        else
        {
            (snapshotResults, snapshotDocIds) = ExecuteFullScan(collectionName, query.Filters);
        }

        List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        return ExecuteQuery(query).Count;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        CollectionEntry collection = _db.GetCollection(collectionName);

        QueryPlan plan = CreateQueryPlan(collection, query.Filters);
        List<T> snapshotResults;
        HashSet<int> snapshotDocIds;

        if (plan.PlanType == QueryPlanType.PrimaryKeyRange)
        {
            (snapshotResults, snapshotDocIds) = await ExecutePrimaryKeyRangeScanAsync(collectionName, plan, query.Filters, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            (snapshotResults, snapshotDocIds) = await ExecuteFullScanAsync(collectionName, query.Filters, cancellationToken).ConfigureAwait(false);
        }

        List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return (await ExecuteQueryAsync(query, cancellationToken).ConfigureAwait(false)).Count;
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

    private (List<T> Results, HashSet<int> DocIds) ExecutePrimaryKeyRangeScan(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = _db.SearchDocIdRange(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
            }
        }

        return (results, docIds);
    }

    private async Task<(List<T> Results, HashSet<int> DocIds)> ExecutePrimaryKeyRangeScanAsync(string collectionName, QueryPlan plan, IReadOnlyList<IFieldFilter> filters, CancellationToken cancellationToken)
    {
        List<T> results = new List<T>();
        HashSet<int> docIds = new HashSet<int>();

        int startId = plan.StartDocId ?? int.MinValue;
        int endId = plan.EndDocId ?? int.MaxValue;

        List<int> rangeDocIds = await _db.SearchDocIdRangeAsync(collectionName, startId, endId, plan.IncludeStart, plan.IncludeEnd, cancellationToken).ConfigureAwait(false);
        List<DocumentVersion> visibleVersions = _versionIndex.GetVisibleVersionsForDocIds(collectionName, rangeDocIds, _snapshotTxId);

        IReadOnlyList<IFieldFilter> remainingFilters = GetRemainingFilters(filters, plan.UsedFilterIndex);

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, remainingFilters))
            {
                results.Add(document);
                docIds.Add(version.DocumentId);
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
                docIds.Add(_typeInfo.IdGetter(document));
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
                docIds.Add(_typeInfo.IdGetter(document));
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
        IReadOnlyDictionary<(string CollectionName, int DocId), WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<T> documentsToAdd = new List<T>();

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
}
