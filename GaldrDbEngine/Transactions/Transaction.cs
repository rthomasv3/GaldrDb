using System;
using System.Collections.Generic;
using System.Text;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

public class Transaction : IDisposable
{
    private readonly GaldrDb _db;
    private readonly TransactionManager _txManager;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly Dictionary<(string CollectionName, int DocId), WriteSetEntry> _writeSet;
    private readonly Dictionary<string, int> _nextIdByCollection;
    private readonly bool _isReadOnly;
    private bool _disposed;

    public TxId TxId { get; }
    public TxId SnapshotTxId { get; }
    public TransactionState State { get; private set; }

    internal Transaction(
        GaldrDb db,
        TransactionManager txManager,
        TxId txId,
        TxId snapshotTxId,
        bool isReadOnly,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _db = db;
        _txManager = txManager;
        TxId = txId;
        SnapshotTxId = snapshotTxId;
        _isReadOnly = isReadOnly;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
        _writeSet = new Dictionary<(string CollectionName, int DocId), WriteSetEntry>();
        _nextIdByCollection = new Dictionary<string, int>();
        State = TransactionState.Active;
        _disposed = false;
    }

    public bool IsReadOnly
    {
        get { return _isReadOnly; }
    }

    public int WriteSetCount
    {
        get { return _writeSet.Count; }
    }

    public T GetById<T>(int id)
    {
        return GetById(id, GaldrTypeRegistry.Get<T>());
    }

    public int Insert<T>(T document)
    {
        return Insert(document, GaldrTypeRegistry.Get<T>());
    }

    public bool Update<T>(T document)
    {
        return Update(document, GaldrTypeRegistry.Get<T>());
    }

    public bool Delete<T>(int id)
    {
        return Delete<T>(id, GaldrTypeRegistry.Get<T>());
    }

    public QueryBuilder<T> Query<T>()
    {
        return Query(GaldrTypeRegistry.Get<T>());
    }

    public QueryBuilder<T> Query<T>(GaldrTypeInfo<T> typeInfo)
    {
        EnsureActive();

        TransactionQueryExecutor<T> executor = new TransactionQueryExecutor<T>(this, _db, typeInfo, _jsonSerializer, _jsonOptions);
        QueryBuilder<T> queryBuilder = new QueryBuilder<T>(executor);

        return queryBuilder;
    }

    public T GetById<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        EnsureActive();

        string collectionName = typeInfo.CollectionName;

        // Check write set first (read your own writes)
        if (_writeSet.TryGetValue((collectionName, id), out WriteSetEntry entry))
        {
            if (entry.Operation == WriteOperation.Delete)
            {
                return default(T);
            }

            string json = Encoding.UTF8.GetString(entry.SerializedData);
            T result = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            return result;
        }

        // Fall back to database
        T document = _db.GetById(id, typeInfo);
        return document;
    }

    public int Insert<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;
        _db.EnsureCollection(typeInfo);

        CollectionEntry collection = _db.GetCollection(collectionName);
        int currentId = typeInfo.IdGetter(document);
        int assignedId;

        if (currentId == 0)
        {
            assignedId = GetNextIdForCollection(collectionName, collection.NextId);
            typeInfo.IdSetter(document, assignedId);
        }
        else
        {
            assignedId = currentId;
        }

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);

        IReadOnlyList<IndexFieldEntry> indexFields = ExtractIndexFields(document, typeInfo);

        WriteSetEntry entry = new WriteSetEntry
        {
            Operation = WriteOperation.Insert,
            CollectionName = collectionName,
            DocumentId = assignedId,
            SerializedData = jsonBytes,
            PreviousLocation = null,
            NewLocation = null,
            IndexFields = indexFields,
            OldIndexFields = null
        };

        _writeSet[(collectionName, assignedId)] = entry;

        return assignedId;
    }

    public bool Update<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        EnsureActive();
        EnsureWritable();

        int id = typeInfo.IdGetter(document);

        if (id == 0)
        {
            throw new InvalidOperationException("Cannot update a document with Id = 0. The document must have a valid Id.");
        }

        string collectionName = typeInfo.CollectionName;
        _db.EnsureCollection(typeInfo);

        // Check if document exists (either in write set or database)
        bool exists = false;
        DocumentLocation previousLocation = null;
        IReadOnlyList<IndexFieldEntry> oldIndexFields = null;

        if (_writeSet.TryGetValue((collectionName, id), out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                previousLocation = existingEntry.PreviousLocation;
                oldIndexFields = existingEntry.IndexFields;
            }
        }
        else
        {
            T existing = _db.GetById(id, typeInfo);
            if (existing != null)
            {
                exists = true;
                oldIndexFields = ExtractIndexFields(existing, typeInfo);
            }
        }

        if (!exists)
        {
            return false;
        }

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);

        IReadOnlyList<IndexFieldEntry> newIndexFields = ExtractIndexFields(document, typeInfo);

        WriteSetEntry entry = new WriteSetEntry
        {
            Operation = WriteOperation.Update,
            CollectionName = collectionName,
            DocumentId = id,
            SerializedData = jsonBytes,
            PreviousLocation = previousLocation,
            NewLocation = null,
            IndexFields = newIndexFields,
            OldIndexFields = oldIndexFields
        };

        _writeSet[(collectionName, id)] = entry;

        return true;
    }

    public bool Delete<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;
        _db.EnsureCollection(typeInfo);

        // Check if document exists
        bool exists = false;
        IReadOnlyList<IndexFieldEntry> oldIndexFields = null;

        if (_writeSet.TryGetValue((collectionName, id), out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                oldIndexFields = existingEntry.IndexFields;
            }
        }
        else
        {
            T existing = _db.GetById(id, typeInfo);
            if (existing != null)
            {
                exists = true;
                oldIndexFields = ExtractIndexFields(existing, typeInfo);
            }
        }

        if (!exists)
        {
            return false;
        }

        WriteSetEntry entry = new WriteSetEntry
        {
            Operation = WriteOperation.Delete,
            CollectionName = collectionName,
            DocumentId = id,
            SerializedData = null,
            PreviousLocation = null,
            NewLocation = null,
            IndexFields = null,
            OldIndexFields = oldIndexFields
        };

        _writeSet[(collectionName, id)] = entry;

        return true;
    }

    public void Commit()
    {
        EnsureActive();

        if (_isReadOnly)
        {
            State = TransactionState.Committed;
            _txManager.MarkCommitted(TxId);
            return;
        }

        State = TransactionState.Committing;

        // Apply write set to database
        foreach (KeyValuePair<(string CollectionName, int DocId), WriteSetEntry> kvp in _writeSet)
        {
            WriteSetEntry entry = kvp.Value;

            switch (entry.Operation)
            {
                case WriteOperation.Insert:
                    _db.CommitInsert(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields);
                    break;

                case WriteOperation.Update:
                    _db.CommitUpdate(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, entry.OldIndexFields);
                    break;

                case WriteOperation.Delete:
                    _db.CommitDelete(entry.CollectionName, entry.DocumentId, entry.OldIndexFields);
                    break;
            }
        }

        State = TransactionState.Committed;
        _txManager.MarkCommitted(TxId);
        _writeSet.Clear();
    }

    public void Rollback()
    {
        if (State == TransactionState.Committed || State == TransactionState.Aborted)
        {
            return;
        }

        _writeSet.Clear();
        State = TransactionState.Aborted;
        _txManager.MarkAborted(TxId);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            if (State == TransactionState.Active || State == TransactionState.Committing)
            {
                Rollback();
            }
        }
    }

    internal IReadOnlyDictionary<(string CollectionName, int DocId), WriteSetEntry> GetWriteSet()
    {
        return _writeSet;
    }

    private void EnsureActive()
    {
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Transaction is not active. Current state: {State}");
        }
    }

    private void EnsureWritable()
    {
        if (_isReadOnly)
        {
            throw new InvalidOperationException("Cannot perform write operation on a read-only transaction");
        }
    }

    private int GetNextIdForCollection(string collectionName, int collectionNextId)
    {
        int nextId;

        if (_nextIdByCollection.TryGetValue(collectionName, out int trackedNextId))
        {
            nextId = trackedNextId;
        }
        else
        {
            nextId = collectionNextId;
        }

        _nextIdByCollection[collectionName] = nextId + 1;

        return nextId;
    }

    private IReadOnlyList<IndexFieldEntry> ExtractIndexFields<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        IndexFieldWriter writer = new IndexFieldWriter();
        typeInfo.ExtractIndexedFields(document, writer);
        return writer.GetFields();
    }
}
