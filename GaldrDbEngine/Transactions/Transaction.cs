using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;
using GaldrJson;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Represents a database transaction with snapshot isolation.
/// Must be committed or disposed when complete.
/// </summary>
public class Transaction : IDisposable
{
    private readonly GaldrDb _db;
    private readonly TransactionManager _txManager;
    private readonly VersionIndex _versionIndex;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly Dictionary<DocumentKey, WriteSetEntry> _writeSet;
    private readonly Dictionary<DocumentKey, TxId> _readSet;
    private readonly Dictionary<string, int> _nextIdByCollection;
    private readonly bool _isReadOnly;
    private bool _disposed;
    private bool _hasActiveWalTransaction;

    /// <summary>
    /// The unique transaction identifier.
    /// </summary>
    public TxId TxId { get; }

    /// <summary>
    /// The snapshot transaction ID that determines visibility of data.
    /// </summary>
    public TxId SnapshotTxId { get; }

    /// <summary>
    /// The current state of the transaction.
    /// </summary>
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
        _writeSet = new Dictionary<DocumentKey, WriteSetEntry>();
        _readSet = new Dictionary<DocumentKey, TxId>();
        _nextIdByCollection = new Dictionary<string, int>();
        State = TransactionState.Active;
        _disposed = false;
    }

    /// <summary>
    /// Whether this is a read-only transaction.
    /// </summary>
    public bool IsReadOnly
    {
        get { return _isReadOnly; }
    }

    /// <summary>
    /// Number of pending writes in this transaction.
    /// </summary>
    public int WriteSetCount
    {
        get { return _writeSet.Count; }
    }

    /// <summary>
    /// Creates a query builder for the specified document type within this transaction.
    /// </summary>
    /// <typeparam name="T">The document type to query.</typeparam>
    /// <returns>A query builder for constructing and executing queries.</returns>
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

    /// <summary>
    /// Gets a document by its ID within this transaction's snapshot.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>The document, or default if not found.</returns>
    public T GetById<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();

        T result = default(T);
        string collectionName = typeInfo.CollectionName;
        DocumentKey key = new DocumentKey(collectionName, id);

        // Check write set first (read your own writes)
        if (_writeSet.TryGetValue(key, out WriteSetEntry entry))
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

                // Track this read for conflict detection at commit time
                if (!_readSet.ContainsKey(key))
                {
                    _readSet[key] = visibleVersion.CreatedBy;
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    /// <exception cref="WriteConflictException">Thrown if a document with the same ID already exists.</exception>
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

            _writeSet[new DocumentKey(collectionName, assignedId)] = entry;

            return assignedId;
        }
        finally
        {
            JsonWriterPool.Return(pooledWriter);
        }
    }

    /// <summary>
    /// Updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    /// <exception cref="WriteConflictException">Thrown if the document was modified by a concurrent transaction.</exception>
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

        // Get the latest version to track what we're reading from
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);

        // Early conflict check
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
        TxId? readVersionTxId = null;
        DocumentKey key = new DocumentKey(collectionName, id);

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                previousLocation = existingEntry.PreviousLocation;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Use the version from our read set if available (for proper conflict detection)
                // Otherwise fall back to the visible version
                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = _db.ReadDocumentByLocation(visibleVersion.Location);
                    CollectionEntry collection = _db.GetCollection(collectionName);
                    oldIndexFields = IndexFieldExtractor.ExtractFromBytes(docBytes, collection.Indexes);
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
                    OldIndexFields = oldIndexFields,
                    ReadVersionTxId = readVersionTxId
                };

                _writeSet[key] = entry;
            }
            finally
            {
                JsonWriterPool.Return(pooledWriter);
            }
        }

        return exists;
    }

    /// <summary>
    /// Creates a partial update builder for updating specific fields of a document by ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>An UpdateBuilder for chaining Set calls.</returns>
    public UpdateBuilder<T> UpdateById<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        return new UpdateBuilder<T>(this, typeInfo, id);
    }

    /// <summary>
    /// Deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    /// <exception cref="WriteConflictException">Thrown if the document was modified by a concurrent transaction.</exception>
    public bool Delete<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

        // Get the latest version to track what we're reading from
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);

        // Early conflict check
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
        TxId? readVersionTxId = null;
        DocumentKey key = new DocumentKey(collectionName, id);

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Use the version from our read set if available (for proper conflict detection)
                // Otherwise fall back to the visible version
                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = _db.ReadDocumentByLocation(visibleVersion.Location);
                    CollectionEntry collection = _db.GetCollection(collectionName);
                    oldIndexFields = IndexFieldExtractor.ExtractFromBytes(docBytes, collection.Indexes);
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
                OldIndexFields = oldIndexFields,
                ReadVersionTxId = readVersionTxId
            };

            _writeSet[key] = entry;
        }

        return exists;
    }

    /// <summary>
    /// Commits all pending changes in this transaction.
    /// </summary>
    /// <exception cref="WriteConflictException">Thrown if a concurrent transaction modified documents in the write set.</exception>
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

            _db.BeginWalTransaction(TxId.Value);
            _hasActiveWalTransaction = true;

            try
            {
                // Phase 1: Write all data to storage, collect version operations
                List<VersionOperation> versionOps = new List<VersionOperation>(_writeSet.Count);

                foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in _writeSet)
                {
                    WriteSetEntry entry = kvp.Value;
                    DocumentLocation location;

                    switch (entry.Operation)
                    {
                        case WriteOperation.Insert:
                            location = _db.CommitInsert(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields);
                            versionOps.Add(VersionOperation.ForInsert(entry.CollectionName, entry.DocumentId, location));
                            break;

                        case WriteOperation.Update:
                            location = _db.CommitUpdate(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, entry.OldIndexFields);
                            versionOps.Add(VersionOperation.ForUpdate(entry.CollectionName, entry.DocumentId, location, entry.ReadVersionTxId));
                            break;

                        case WriteOperation.Delete:
                            _db.CommitDelete(entry.CollectionName, entry.DocumentId, entry.OldIndexFields);
                            versionOps.Add(VersionOperation.ForDelete(entry.CollectionName, entry.DocumentId, entry.ReadVersionTxId));
                            break;
                    }
                }

                // Phase 2: Atomically validate and add all versions
                // This ensures no concurrent transaction modified our documents
                _versionIndex.ValidateAndAddVersions(TxId, SnapshotTxId, versionOps);

                // Phase 3: Commit WAL
                _db.CommitWalTransaction();
                _hasActiveWalTransaction = false;

                State = TransactionState.Committed;
                _txManager.MarkCommitted(TxId);
                _writeSet.Clear();

                _db.TryRunGarbageCollection();
                _db.TryRunAutoCheckpoint();
            }
            catch
            {
                _db.AbortWalTransaction();
                _hasActiveWalTransaction = false;
                State = TransactionState.Aborted;
                _txManager.MarkAborted(TxId);
                throw;
            }
        }
    }

    /// <summary>
    /// Rolls back all pending changes and aborts this transaction.
    /// </summary>
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

    /// <summary>
    /// Asynchronously gets a document by its ID within this transaction's snapshot.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or default if not found.</returns>
    public async Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();

        T result = default(T);
        string collectionName = typeInfo.CollectionName;
        DocumentKey key = new DocumentKey(collectionName, id);

        // Check write set first (read your own writes)
        if (_writeSet.TryGetValue(key, out WriteSetEntry entry))
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

    /// <summary>
    /// Asynchronously inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
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

            _writeSet[new DocumentKey(collectionName, assignedId)] = entry;

            return Task.FromResult(assignedId);
        }
        finally
        {
            JsonWriterPool.Return(pooledWriter);
        }
    }

    /// <summary>
    /// Asynchronously updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
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

        // Get the latest version to track what we're reading from
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);

        // Early conflict check
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
        TxId? readVersionTxId = null;
        DocumentKey key = new DocumentKey(collectionName, id);

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                previousLocation = existingEntry.PreviousLocation;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Use the version from our read set if available (for proper conflict detection)
                // Otherwise fall back to the visible version
                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);
                    CollectionEntry collection = _db.GetCollection(collectionName);
                    oldIndexFields = IndexFieldExtractor.ExtractFromBytes(docBytes, collection.Indexes);
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
                    OldIndexFields = oldIndexFields,
                    ReadVersionTxId = readVersionTxId
                };

                _writeSet[key] = entry;
            }
            finally
            {
                JsonWriterPool.Return(pooledWriter);
            }
        }

        return exists;
    }

    /// <summary>
    /// Asynchronously deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    public async Task<bool> DeleteAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();

        EnsureActive();
        EnsureWritable();

        string collectionName = typeInfo.CollectionName;

        // Get the latest version to track what we're reading from
        DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, id);

        // Early conflict check
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
        TxId? readVersionTxId = null;
        DocumentKey key = new DocumentKey(collectionName, id);

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, id, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;

                // Use the version from our read set if available (for proper conflict detection)
                // Otherwise fall back to the visible version
                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }

                // Only read document if there are indexes to clean up
                if (typeInfo.IndexedFieldNames.Count > 0)
                {
                    byte[] docBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);
                    CollectionEntry collection = _db.GetCollection(collectionName);
                    oldIndexFields = IndexFieldExtractor.ExtractFromBytes(docBytes, collection.Indexes);
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
                OldIndexFields = oldIndexFields,
                ReadVersionTxId = readVersionTxId
            };

            _writeSet[key] = entry;
        }

        return exists;
    }

    /// <summary>
    /// Asynchronously commits all pending changes in this transaction.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="WriteConflictException">Thrown if a concurrent transaction modified documents in the write set.</exception>
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

            _db.BeginWalTransaction(TxId.Value);
            _hasActiveWalTransaction = true;

            try
            {
                // Phase 1: Write all data to storage, collect version operations
                List<VersionOperation> versionOps = new List<VersionOperation>();

                foreach (KeyValuePair<DocumentKey, WriteSetEntry> kvp in _writeSet)
                {
                    WriteSetEntry entry = kvp.Value;
                    DocumentLocation location;

                    switch (entry.Operation)
                    {
                        case WriteOperation.Insert:
                            location = await _db.CommitInsertAsync(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, cancellationToken).ConfigureAwait(false);
                            versionOps.Add(VersionOperation.ForInsert(entry.CollectionName, entry.DocumentId, location));
                            break;

                        case WriteOperation.Update:
                            location = await _db.CommitUpdateAsync(entry.CollectionName, entry.DocumentId, entry.SerializedData, entry.IndexFields, entry.OldIndexFields, cancellationToken).ConfigureAwait(false);
                            versionOps.Add(VersionOperation.ForUpdate(entry.CollectionName, entry.DocumentId, location, entry.ReadVersionTxId));
                            break;

                        case WriteOperation.Delete:
                            await _db.CommitDeleteAsync(entry.CollectionName, entry.DocumentId, entry.OldIndexFields, cancellationToken).ConfigureAwait(false);
                            versionOps.Add(VersionOperation.ForDelete(entry.CollectionName, entry.DocumentId, entry.ReadVersionTxId));
                            break;
                    }
                }

                // Phase 2: Atomically validate and add all versions
                _versionIndex.ValidateAndAddVersions(TxId, SnapshotTxId, versionOps);

                // Phase 3: Commit WAL
                _db.CommitWalTransaction();
                _hasActiveWalTransaction = false;

                State = TransactionState.Committed;
                _txManager.MarkCommitted(TxId);
                _writeSet.Clear();

                _db.TryRunGarbageCollection();

                _db.TryRunAutoCheckpoint();
            }
            catch
            {
                _db.AbortWalTransaction();
                _hasActiveWalTransaction = false;
                State = TransactionState.Aborted;
                _txManager.MarkAborted(TxId);
                throw;
            }
        }
    }

    /// <summary>
    /// Disposes the transaction. If not committed, the transaction is rolled back.
    /// </summary>
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

    internal IReadOnlyDictionary<DocumentKey, WriteSetEntry> GetWriteSet()
    {
        return _writeSet;
    }

    internal void RecordRead(string collectionName, int docId, TxId versionCreatedBy)
    {
        DocumentKey key = new DocumentKey(collectionName, docId);
        if (!_readSet.ContainsKey(key))
        {
            _readSet[key] = versionCreatedBy;
        }
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

    internal bool ExecutePartialUpdate<T>(
        GaldrTypeInfo<T> typeInfo,
        int documentId,
        List<FieldModification> modifications)
    {
        string collectionName = typeInfo.CollectionName;
        DocumentKey key = new DocumentKey(collectionName, documentId);

        // Check if document exists and get current data
        bool exists = false;
        byte[] existingBytes = null;
        IReadOnlyList<IndexFieldEntry> oldIndexFields = null;
        TxId? readVersionTxId = null;

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                existingBytes = existingEntry.SerializedData;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, documentId);

            if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
            {
                throw new WriteConflictException(
                    $"Document {collectionName}/{documentId} was modified by a concurrent transaction",
                    collectionName,
                    documentId,
                    latestVersion.CreatedBy);
            }

            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, documentId, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;
                existingBytes = _db.ReadDocumentByLocation(visibleVersion.Location);

                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }
            }
        }

        if (exists)
        {
            // Check if any indexed fields are being modified
            bool hasIndexedFieldModifications = HasIndexedFieldModifications(modifications, typeInfo.IndexedFieldNames);

            // Extract old index fields if needed
            if (hasIndexedFieldModifications && oldIndexFields == null && typeInfo.IndexedFieldNames.Count > 0)
            {
                CollectionEntry collection = _db.GetCollection(collectionName);
                oldIndexFields = IndexFieldExtractor.ExtractFromBytes(existingBytes, collection.Indexes);
            }

            // Parse, modify, and serialize
            JsonDocument doc = JsonDocument.Parse(existingBytes);
            ApplyModifications(doc, modifications);
            byte[] newBytes = doc.ToUtf8Bytes();

            // Extract new index fields
            IReadOnlyList<IndexFieldEntry> newIndexFields;
            if (hasIndexedFieldModifications)
            {
                CollectionEntry collection = _db.GetCollection(collectionName);
                newIndexFields = IndexFieldExtractor.ExtractFromBytes(newBytes, collection.Indexes);
            }
            else
            {
                newIndexFields = oldIndexFields;
            }

            // Add to write set
            WriteSetEntry entry = new WriteSetEntry
            {
                Operation = WriteOperation.Update,
                CollectionName = collectionName,
                DocumentId = documentId,
                SerializedData = newBytes,
                PreviousLocation = null,
                NewLocation = null,
                IndexFields = newIndexFields,
                OldIndexFields = oldIndexFields,
                ReadVersionTxId = readVersionTxId
            };

            _writeSet[key] = entry;
        }

        return exists;
    }

    internal async Task<bool> ExecutePartialUpdateAsync<T>(
        GaldrTypeInfo<T> typeInfo,
        int documentId,
        List<FieldModification> modifications,
        CancellationToken cancellationToken)
    {
        string collectionName = typeInfo.CollectionName;
        DocumentKey key = new DocumentKey(collectionName, documentId);

        // Check if document exists and get current data
        bool exists = false;
        byte[] existingBytes = null;
        IReadOnlyList<IndexFieldEntry> oldIndexFields = null;
        TxId? readVersionTxId = null;

        if (_writeSet.TryGetValue(key, out WriteSetEntry existingEntry))
        {
            if (existingEntry.Operation != WriteOperation.Delete)
            {
                exists = true;
                existingBytes = existingEntry.SerializedData;
                oldIndexFields = existingEntry.IndexFields;
                readVersionTxId = existingEntry.ReadVersionTxId;
            }
        }
        else
        {
            DocumentVersion latestVersion = _versionIndex.GetLatestVersion(collectionName, documentId);

            if (latestVersion != null && latestVersion.CreatedBy > SnapshotTxId)
            {
                throw new WriteConflictException(
                    $"Document {collectionName}/{documentId} was modified by a concurrent transaction",
                    collectionName,
                    documentId,
                    latestVersion.CreatedBy);
            }

            DocumentVersion visibleVersion = _versionIndex.GetVisibleVersion(collectionName, documentId, SnapshotTxId);

            if (visibleVersion != null)
            {
                exists = true;
                existingBytes = await _db.ReadDocumentByLocationAsync(visibleVersion.Location, cancellationToken).ConfigureAwait(false);

                if (_readSet.TryGetValue(key, out TxId trackedReadVersion))
                {
                    readVersionTxId = trackedReadVersion;
                }
                else
                {
                    readVersionTxId = visibleVersion.CreatedBy;
                }
            }
        }

        if (exists)
        {
            // Check if any indexed fields are being modified
            bool hasIndexedFieldModifications = HasIndexedFieldModifications(modifications, typeInfo.IndexedFieldNames);

            // Extract old index fields if needed
            if (hasIndexedFieldModifications && oldIndexFields == null && typeInfo.IndexedFieldNames.Count > 0)
            {
                CollectionEntry collection = _db.GetCollection(collectionName);
                oldIndexFields = IndexFieldExtractor.ExtractFromBytes(existingBytes, collection.Indexes);
            }

            // Parse, modify, and serialize
            JsonDocument doc = JsonDocument.Parse(existingBytes);
            ApplyModifications(doc, modifications);
            byte[] newBytes = doc.ToUtf8Bytes();

            // Extract new index fields
            IReadOnlyList<IndexFieldEntry> newIndexFields;
            if (hasIndexedFieldModifications)
            {
                CollectionEntry collection = _db.GetCollection(collectionName);
                newIndexFields = IndexFieldExtractor.ExtractFromBytes(newBytes, collection.Indexes);
            }
            else
            {
                newIndexFields = oldIndexFields;
            }

            // Add to write set
            WriteSetEntry entry = new WriteSetEntry
            {
                Operation = WriteOperation.Update,
                CollectionName = collectionName,
                DocumentId = documentId,
                SerializedData = newBytes,
                PreviousLocation = null,
                NewLocation = null,
                IndexFields = newIndexFields,
                OldIndexFields = oldIndexFields,
                ReadVersionTxId = readVersionTxId
            };

            _writeSet[key] = entry;
        }

        return exists;
    }

    private bool HasIndexedFieldModifications(
        List<FieldModification> modifications,
        IReadOnlyList<string> indexedFieldNames)
    {
        bool result = false;

        for (int i = 0; i < modifications.Count && !result; i++)
        {
            string modifiedField = modifications[i].FieldName;
            for (int j = 0; j < indexedFieldNames.Count && !result; j++)
            {
                if (modifiedField == indexedFieldNames[j])
                {
                    result = true;
                }
            }
        }

        return result;
    }

    private void ApplyModifications(JsonDocument doc, List<FieldModification> modifications)
    {
        for (int i = 0; i < modifications.Count; i++)
        {
            FieldModification mod = modifications[i];

            if (mod.Value == null)
            {
                doc.SetNull(mod.FieldName);
            }
            else
            {
                switch (mod.FieldType)
                {
                    case GaldrFieldType.String:
                        doc.SetString(mod.FieldName, (string)mod.Value);
                        break;
                    case GaldrFieldType.Int32:
                        doc.SetInt32(mod.FieldName, (int)mod.Value);
                        break;
                    case GaldrFieldType.Int64:
                        doc.SetInt64(mod.FieldName, (long)mod.Value);
                        break;
                    case GaldrFieldType.Double:
                        doc.SetDouble(mod.FieldName, (double)mod.Value);
                        break;
                    case GaldrFieldType.Decimal:
                        doc.SetDecimal(mod.FieldName, (decimal)mod.Value);
                        break;
                    case GaldrFieldType.Boolean:
                        doc.SetBoolean(mod.FieldName, (bool)mod.Value);
                        break;
                    case GaldrFieldType.DateTime:
                        doc.SetDateTime(mod.FieldName, (DateTime)mod.Value);
                        break;
                    case GaldrFieldType.DateTimeOffset:
                        doc.SetDateTimeOffset(mod.FieldName, (DateTimeOffset)mod.Value);
                        break;
                    case GaldrFieldType.Guid:
                        doc.SetGuid(mod.FieldName, (Guid)mod.Value);
                        break;
                    default:
                        throw new NotSupportedException($"Partial update for field type {mod.FieldType} is not yet supported");
                }
            }
        }
    }
}
