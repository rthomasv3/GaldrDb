using System.Collections.Generic;
using System.Text;
using GaldrDbEngine.Query;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

public sealed class TransactionQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly GaldrTypeInfo<T> _typeInfo;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly DatabaseQueryExecutor<T> _dbExecutor;

    public TransactionQueryExecutor(
        Transaction transaction,
        GaldrDb db,
        GaldrTypeInfo<T> typeInfo,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _transaction = transaction;
        _db = db;
        _typeInfo = typeInfo;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
        _dbExecutor = new DatabaseQueryExecutor<T>(db, typeInfo);
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        // Execute query against database
        List<T> dbResults = _dbExecutor.ExecuteQuery(query);

        // Apply write set overlay
        List<T> results = ApplyWriteSetOverlay(dbResults, query.Filters);

        return results;
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        List<T> results = ExecuteQuery(query);
        return results.Count;
    }

    private List<T> ApplyWriteSetOverlay(List<T> dbResults, IReadOnlyList<IFieldFilter> filters)
    {
        string collectionName = _typeInfo.CollectionName;
        IReadOnlyDictionary<(string CollectionName, int DocId), WriteSetEntry> writeSet = _transaction.GetWriteSet();

        // Build sets of IDs to remove (deleted or updated) and documents to add
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
                T document = DeserializeDocument(entry.SerializedData);
                if (PassesFilters(document, filters))
                {
                    documentsToAdd.Add(document);
                }
            }
        }

        // Build result: db results minus removed, plus added
        List<T> results = new List<T>();

        foreach (T document in dbResults)
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
        T document = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
        return document;
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
}
