using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

public sealed class TransactionQueryExecutor<T> : IQueryExecutor<T>
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

        // Get all visible versions from VersionIndex at our snapshot
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        // Load documents from visible versions
        List<T> snapshotResults = new List<T>();
        HashSet<int> snapshotDocIds = new HashSet<int>();

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = _db.ReadDocumentByLocation(version.Location);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, query.Filters))
            {
                snapshotResults.Add(document);
                snapshotDocIds.Add(_typeInfo.IdGetter(document));
            }
        }

        // Apply write set overlay
        List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);

        // Apply ordering
        List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);

        // Apply skip and limit
        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        return ExecuteQuery(query).Count;
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        string collectionName = _typeInfo.CollectionName;
        List<DocumentVersion> visibleVersions = _versionIndex.GetAllVisibleVersions(collectionName, _snapshotTxId);

        List<T> snapshotResults = new List<T>();
        HashSet<int> snapshotDocIds = new HashSet<int>();

        foreach (DocumentVersion version in visibleVersions)
        {
            byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(version.Location, cancellationToken).ConfigureAwait(false);
            T document = _jsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(jsonBytes), _jsonOptions);

            if (PassesFilters(document, query.Filters))
            {
                snapshotResults.Add(document);
                snapshotDocIds.Add(_typeInfo.IdGetter(document));
            }
        }

        List<T> merged = ApplyWriteSetOverlay(snapshotResults, snapshotDocIds, query.Filters);
        List<T> sorted = ApplyOrdering(merged, query.OrderByClauses);

        return ApplySkipAndLimit(sorted, query.SkipValue, query.LimitValue);
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return (await ExecuteQueryAsync(query, cancellationToken).ConfigureAwait(false)).Count;
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
