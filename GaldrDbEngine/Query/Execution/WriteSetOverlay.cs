using System.Collections.Generic;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query.Execution;

internal sealed class WriteSetOverlay<TDocument>
{
    private readonly Transaction _transaction;
    private readonly string _collectionName;
    private readonly IDocumentReader<TDocument> _reader;

    public WriteSetOverlay(Transaction transaction, string collectionName, IDocumentReader<TDocument> reader)
    {
        _transaction = transaction;
        _collectionName = collectionName;
        _reader = reader;
    }

    public List<TDocument> Apply(
        List<TDocument> snapshotResults,
        HashSet<int> snapshotDocIds,
        IReadOnlyList<IFieldFilter> filters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();

        HashSet<int> idsToRemove = new HashSet<int>();
        List<TDocument> documentsToAdd = new List<TDocument>();

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
                TDocument document = _reader.ReadDocument(entry.SerializedData);
                if (_reader.PassesFilters(document, filters))
                {
                    documentsToAdd.Add(document);
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!snapshotDocIds.Contains(docId))
                {
                    TDocument document = _reader.ReadDocument(entry.SerializedData);
                    if (_reader.PassesFilters(document, filters))
                    {
                        documentsToAdd.Add(document);
                    }
                }
            }
        }

        List<TDocument> results = new List<TDocument>();

        foreach (TDocument document in snapshotResults)
        {
            int docId = _reader.GetDocumentId(document);
            if (!idsToRemove.Contains(docId))
            {
                results.Add(document);
            }
        }

        results.AddRange(documentsToAdd);

        return results;
    }

    public int GetCountAdjustment(HashSet<int> countedDocIds, IReadOnlyList<IFieldFilter> filters)
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        int adjustment = 0;

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
                bool wasInSnapshot = countedDocIds.Contains(docId);
                TDocument document = _reader.ReadDocument(entry.SerializedData);
                bool passesNow = _reader.PassesFilters(document, filters);

                if (wasInSnapshot && !passesNow)
                {
                    adjustment--;
                }
                else if (!wasInSnapshot && passesNow)
                {
                    adjustment++;
                }
            }
            else if (entry.Operation == WriteOperation.Insert)
            {
                if (!countedDocIds.Contains(docId))
                {
                    TDocument document = _reader.ReadDocument(entry.SerializedData);
                    if (_reader.PassesFilters(document, filters))
                    {
                        adjustment++;
                    }
                }
            }
        }

        return adjustment;
    }

    public HashSet<int> GetDeletedDocIds()
    {
        IReadOnlyDictionary<DocumentKey, WriteSetEntry> writeSet = _transaction.GetWriteSet();
        HashSet<int> deletedDocIds = new HashSet<int>();

        foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in writeSet)
        {
            if (kvp.Key.CollectionName != _collectionName)
            {
                continue;
            }

            if (kvp.Value.Operation == WriteOperation.Delete || kvp.Value.Operation == WriteOperation.Update)
            {
                deletedDocIds.Add(kvp.Key.DocId);
            }
        }

        return deletedDocIds;
    }

    public bool HasAnyMatchingWriteSetDocument(IReadOnlyList<IFieldFilter> filters)
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
                TDocument document = _reader.ReadDocument(entry.SerializedData);
                if (_reader.PassesFilters(document, filters))
                {
                    found = true;
                    break;
                }
            }
        }

        return found;
    }
}
