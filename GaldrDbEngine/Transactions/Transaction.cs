using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

public class Transaction : IDisposable
{
    private readonly GaldrDb _db;
    private readonly TransactionManager _txManager;
    private readonly VersionIndex _versionIndex;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly Dictionary<(string CollectionName, int DocId), WriteSetEntry> _writeSet;
    private readonly Dictionary<string, int> _nextIdByCollection;
    private readonly bool _isReadOnly;
    private bool _disposed;
    private bool _hasActiveWalTransaction;

    public TxId TxId { get; }
    public TxId SnapshotTxId { get; }
    public TransactionState State { get; private set; }

    internal Transaction(
        GaldrDb db,
        TransactionManager txManager,
        VersionIndex versionIndex,
        TxId txId,
        TxId snapshotTxId,
        bool isReadOnly,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _db = db;
        _txManager = txManager;
        _versionIndex = versionIndex;
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

    public QueryBuilder<T> Query<T>()
    {
        EnsureActive();

        IGaldrTypeInfo typeInfo = GaldrTypeRegistry.Get(typeof(T));
        IQueryExecutor<T> executor;

        if (typeInfo is IGaldrProjectionTypeInfo projTypeInfo)
        {
            executor = new ProjectionQueryExecutor<T>(
                this,
                _db,
                _versionIndex,
                SnapshotTxId,
                projTypeInfo,
                _jsonSerializer,
                _jsonOptions);
        }
        else
        {
            executor = new TransactionQueryExecutor<T>(
                this,
                _db,
                _versionIndex,
                SnapshotTxId,
                (GaldrTypeInfo<T>)typeInfo,
                _jsonSerializer,
                _jsonOptions);
        }

        QueryBuilder<T> queryBuilder = new QueryBuilder<T>(executor);

        return queryBuilder;
    }

    public T GetById<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        
        EnsureActive();

        T result = default(T);
        string collectionName = typeInfo.CollectionName;

        // Check write set first (read your own writes)
        if (_writeSet.TryGetValue((collectionName, id), out WriteSetEntry entry))
        {
            if (entry.Operation != WriteOperation.Delete)
            {
                string json = Encoding.UTF8.GetString(entry.SerializedData);
                result = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            }
        }
        else
        {
            // Check VersionIndex for visible version at our snapshot
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                byte[] jsonBytes = _db.ReadDocumentByLocation(visibleVersion.Location);
                string jsonStr = Encoding.UTF8.GetString(jsonBytes);
                result = _jsonSerializer.Deserialize<T>(jsonStr, _jsonOptions);
            }
        }

        return result;
    }

    public int Insert<T>(T document)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

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

            // Check for write-write conflict on explicit ID insert
            DocumentVersion existingVersion = _versionIndex.GetLatestVersion(collectionName, assignedId);
            if (existingVersion != null && !existingVersion.IsDeleted)
            {
                throw new WriteConflictException(
                    $"Document {collectionName}/{assignedId} already exists",
                    collectionName,
                    assignedId,
                    existingVersion.CreatedBy);
            }
        }

        PooledJsonWriter pooledWriter = JsonWriterPool.Rent();
        try
        {
            _jsonSerializer.SerializeTo(pooledWriter.Writer, document, _jsonOptions);
            byte[] jsonBytes = pooledWriter.WrittenSpan.ToArray();

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
        finally
        {
            JsonWriterPool.Return(pooledWriter);
        }
    }

    public bool Update<T>(T document)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        
        EnsureActive();
        EnsureWritable();

        int id = typeInfo.IdGetter(document);

        if (id == 0)
        {
            throw new InvalidOperationException("Cannot update a document with Id = 0. The document must have a valid Id.");
        }

        string collectionName = typeInfo.CollectionName;

        // Check for write-write conflict
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);
        if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
        {
            throw new WriteConflictException(
                $"Document {collectionName}/{id} was modified by a concurrent transaction",
                collectionName,
                id,
                latestVersion.CreatedBy);
        }

        // Check if document exists (either in write set or database)
        bool exists = false;
        DocumentLocation? previousLocation = null;
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
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = _db.ReadDocumentByLocation(visibleVersion.Location);
                    string json = Encoding.UTF8.GetString(docBytes);
                    T existing = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
                    oldIndexFields = ExtractIndexFields(existing, typeInfo);
                }
            }
        }

        if (exists)
        {
            PooledJsonWriter pooledWriter = JsonWriterPool.Rent();
            try
            {
                _jsonSerializer.SerializeTo(pooledWriter.Writer, document, _jsonOptions);
                byte[] jsonBytes = pooledWriter.WrittenSpan.ToArray();

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
            }
            finally
            {
                JsonWriterPool.Return(pooledWriter);
            }
        }

        return exists;
    }

    public bool Delete<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        
        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

        // Check for write-write conflict
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);
        if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
        {
            throw new WriteConflictException(
                $"Document {collectionName}/{id} was modified by a concurrent transaction",
                collectionName,
                id,
                latestVersion.CreatedBy);
        }

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
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);
            
            if (visibleVersion != null)
            {
                exists = true;

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = _db.ReadDocumentByLocation(visibleVersion.Location);
                    string json = Encoding.UTF8.GetString(docBytes);
                    T existing = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
                    oldIndexFields = ExtractIndexFields(existing, typeInfo);
                }
            }
        }

        if (exists)
        {
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
        }

        return exists;
    }

    public void Commit()
    {
        EnsureActive();

        if (_isReadOnly)
        {
            State = TransactionState.Committed;
            _txManager.MarkCommitted(TxId);
        }
        else
        {
            State = TransactionState.Committing;

            // Final validation: check for write-write conflicts
            ValidateWriteSet();

            // Begin WAL transaction - all page writes will be batched
            _db.BeginWalTransaction(TxId.Value);
            _hasActiveWalTransaction = true;

            try
            {
                // Apply write set to database and update version index
                foreach (KeyValuePair<(string CollectionName, int DocId), WriteSetEntry> kvp in _writeSet)
                {
                    WriteSetEntry entry = kvp.Value;
                    DocumentLocation location;

                    switch (entry.Operation)
                    {
                        case WriteOperation.Insert:
                            location = _db.CommitInsert(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields);
                            _versionIndex.AddVersion(entry.CollectionName, entry.DocumentId, TxId, location);
                            break;

                        case WriteOperation.Update:
                            location = _db.CommitUpdate(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, entry.OldIndexFields);
                            _versionIndex.AddVersion(entry.CollectionName, entry.DocumentId, TxId, location);
                            break;

                        case WriteOperation.Delete:
                            _db.CommitDelete(entry.CollectionName, entry.DocumentId, entry.OldIndexFields);
                            _versionIndex.MarkDeleted(entry.CollectionName, entry.DocumentId, TxId);
                            break;
                    }
                }

                // Commit WAL transaction - writes all batched pages with commit flag and fsyncs
                _db.CommitWalTransaction();
                _hasActiveWalTransaction = false;

                State = TransactionState.Committed;
                _txManager.MarkCommitted(TxId);
                _writeSet.Clear();

                // Try to run garbage collection if threshold is met
                _db.TryRunGarbageCollection();

                // Try to run auto-checkpoint if WAL threshold is met
                _db.TryRunAutoCheckpoint();
            }
            catch
            {
                // Abort WAL transaction on any failure
                _db.AbortWalTransaction();
                _hasActiveWalTransaction = false;
                State = TransactionState.Aborted;
                _txManager.MarkAborted(TxId);
                throw;
            }
        }
    }

    private void ValidateWriteSet()
    {
        foreach (KeyValuePair<(string CollectionName, int DocId), WriteSetEntry> kvp in _writeSet)
        {
            string collectionName = kvp.Key.CollectionName;
            int docId = kvp.Key.DocId;
            WriteSetEntry entry = kvp.Value;

            DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, docId);

            if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
            {
                throw new WriteConflictException(
                    $"Document {collectionName}/{docId} was modified by transaction {latestVersion.CreatedBy}",
                    collectionName,
                    docId,
                    latestVersion.CreatedBy);
            }
        }
    }

    public void Rollback()
    {
        if (State != TransactionState.Committed && State != TransactionState.Aborted)
        {
            // Only abort WAL transaction if this transaction started one
            if (_hasActiveWalTransaction)
            {
                _db.AbortWalTransaction();
                _hasActiveWalTransaction = false;
            }

            _writeSet.Clear();
            State = TransactionState.Aborted;
            _txManager.MarkAborted(TxId);
        }
    }

    public async Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        
        EnsureActive();

        T result = default(T);
        string collectionName = typeInfo.CollectionName;

        // Check write set first (read your own writes)
        if (_writeSet.TryGetValue((collectionName, id), out WriteSetEntry entry))
        {
            if (entry.Operation != WriteOperation.Delete)
            {
                string json = Encoding.UTF8.GetString(entry.SerializedData);
                result = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            }
        }
        else
        {
            // Check VersionIndex for visible version at our snapshot
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                byte[] jsonBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);
                string jsonStr = Encoding.UTF8.GetString(jsonBytes);
                result = _jsonSerializer.Deserialize<T>(jsonStr, _jsonOptions);
            }
        }

        return result;
    }

    public Task<int> InsertAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        
        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

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

            // Check for write-write conflict on explicit ID insert
            DocumentVersion existingVersion = _versionIndex.GetLatestVersion(collectionName, assignedId);
            if (existingVersion != null && !existingVersion.IsDeleted)
            {
                throw new WriteConflictException(
                    $"Document {collectionName}/{assignedId} already exists",
                    collectionName,
                    assignedId,
                    existingVersion.CreatedBy);
            }
        }

        PooledJsonWriter pooledWriter = JsonWriterPool.Rent();
        try
        {
            _jsonSerializer.SerializeTo(pooledWriter.Writer, document, _jsonOptions);
            byte[] jsonBytes = pooledWriter.WrittenSpan.ToArray();

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

            return Task.FromResult(assignedId);
        }
        finally
        {
            JsonWriterPool.Return(pooledWriter);
        }
    }

    public async Task<bool> UpdateAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        int id = typeInfo.IdGetter(document);

        if (id == 0)
        {
            throw new InvalidOperationException("Cannot update a document with Id = 0. The document must have a valid Id.");
        }

        string collectionName = typeInfo.CollectionName;

        // Check for write-write conflict
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);
        if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
        {
            throw new WriteConflictException(
                $"Document {collectionName}/{id} was modified by a concurrent transaction",
                collectionName,
                id,
                latestVersion.CreatedBy);
        }

        // Check if document exists (either in write set or database)
        bool exists = false;
        DocumentLocation? previousLocation = null;
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
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);
                    string json = Encoding.UTF8.GetString(docBytes);
                    T existing = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
                    oldIndexFields = ExtractIndexFields(existing, typeInfo);
                }
            }
        }

        if (exists)
        {
            PooledJsonWriter pooledWriter = JsonWriterPool.Rent();
            try
            {
                _jsonSerializer.SerializeTo(pooledWriter.Writer, document, _jsonOptions);
                byte[] jsonBytes = pooledWriter.WrittenSpan.ToArray();

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
            }
            finally
            {
                JsonWriterPool.Return(pooledWriter);
            }
        }

        return exists;
    }

    public async Task<bool> DeleteAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

        // Check for write-write conflict
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);
        if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
        {
            throw new WriteConflictException(
                $"Document {collectionName}/{id} was modified by a concurrent transaction",
                collectionName,
                id,
                latestVersion.CreatedBy);
        }

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
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);
            
            if (visibleVersion != null)
            {
                exists = true;

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);
                    string json = Encoding.UTF8.GetString(docBytes);
                    T existing = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
                    oldIndexFields = ExtractIndexFields(existing, typeInfo);
                }
            }
        }

        if (exists)
        {
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
        }

        return exists;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        EnsureActive();

        if (_isReadOnly)
        {
            State = TransactionState.Committed;
            _txManager.MarkCommitted(TxId);
        }
        else
        {
            State = TransactionState.Committing;

            // Final validation: check for write-write conflicts
            ValidateWriteSet();

            // Begin WAL transaction - all page writes will be batched
            _db.BeginWalTransaction(TxId.Value);
            _hasActiveWalTransaction = true;

            try
            {
                // Apply write set to database and update version index
                foreach (KeyValuePair<(string CollectionName, int DocId), WriteSetEntry> kvp in _writeSet)
                {
                    WriteSetEntry entry = kvp.Value;
                    DocumentLocation location;

                    switch (entry.Operation)
                    {
                        case WriteOperation.Insert:
                            location = await _db.CommitInsertAsync(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, cancellationToken).ConfigureAwait(false);
                            _versionIndex.AddVersion(entry.CollectionName, entry.DocumentId, TxId, location);
                            break;

                        case WriteOperation.Update:
                            location = await _db.CommitUpdateAsync(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, entry.OldIndexFields, cancellationToken).ConfigureAwait(false);
                            _versionIndex.AddVersion(entry.CollectionName, entry.DocumentId, TxId, location);
                            break;

                        case WriteOperation.Delete:
                            await _db.CommitDeleteAsync(entry.CollectionName, entry.DocumentId, entry.OldIndexFields, cancellationToken).ConfigureAwait(false);
                            _versionIndex.MarkDeleted(entry.CollectionName, entry.DocumentId, TxId);
                            break;
                    }
                }

                // Commit WAL transaction - writes all batched pages with commit flag and fsyncs
                _db.CommitWalTransaction();
                _hasActiveWalTransaction = false;

                State = TransactionState.Committed;
                _txManager.MarkCommitted(TxId);
                _writeSet.Clear();

                // Try to run garbage collection if threshold is met
                _db.TryRunGarbageCollection();

                // Try to run auto-checkpoint if WAL threshold is met
                _db.TryRunAutoCheckpoint();
            }
            catch
            {
                // Abort WAL transaction on any failure
                _db.AbortWalTransaction();
                _hasActiveWalTransaction = false;
                State = TransactionState.Aborted;
                _txManager.MarkAborted(TxId);
                throw;
            }
        }
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
