using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Json;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Query;
using GaldrDbEngine.Query.Execution;
using GaldrDbEngine.Schema;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;
using GaldrDbEngine.WAL;
using GaldrJson;

namespace GaldrDbEngine;

/// <summary>
/// The main database class. Use <see cref="Create"/> to create a new database, <see cref="Open"/> to open an existing one,
/// or <see cref="OpenOrCreate"/> to open or create as needed.
/// </summary>
public class GaldrDb : IGaldrDb
{
    #region Fields

    private readonly string _filePath;
    private readonly GaldrDbOptions _options;
    private readonly string _walPath;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly HashSet<string> _ensuredCollections;
    private readonly object _ddlLock;
    private readonly object _documentCountLock;
    private readonly object _commitSerializationLock;
    private readonly ConcurrentDictionary<string, SecondaryIndexBTree> _secondaryIndexCache;
    private readonly ConcurrentDictionary<string, BTree> _primaryBTreeCache;
    private IPageIO _basePageIO;
    private LruPageCache _pageCache;
    private IPageIO _pageIO;
    private WriteAheadLog _wal;
    private WalPageIO _walPageIO;
    private PageManager _pageManager;
    private DocumentStorage _documentStorage;
    private PageLockManager _pageLockManager;
    private CollectionsMetadata _collectionsMetadata;
    private TransactionManager _txManager;
    private VersionIndex _versionIndex;
    private VersionGarbageCollector _garbageCollector;
    private long _lastGCCommitCount;
    private byte[] _encryptionKey;

    // Track pending root updates per transaction (applied after WAL commit)
    private readonly AsyncLocal<PendingRootUpdates> _pendingRootUpdates;

    #endregion

    #region Internal Properties (for testing)

    internal WriteAheadLog Wal => _wal;
    internal VersionIndex VersionIndex => _versionIndex;
    internal CollectionsMetadata CollectionsMetadata => _collectionsMetadata;

    #endregion

    #region Constructor

    internal GaldrDb(string filePath, GaldrDbOptions options)
    {
        _filePath = filePath;
        _options = options ?? new GaldrDbOptions();
        string directory = Path.GetDirectoryName(filePath);
        string fileNameWithoutExt = Path.GetFileNameWithoutExtension(filePath);
        if (string.IsNullOrEmpty(directory))
        {
            _walPath = $"{fileNameWithoutExt}.wal";
        }
        else
        {
            _walPath = Path.Combine(directory, $"{fileNameWithoutExt}.wal");
        }
        _jsonSerializer = new GaldrJsonSerializer();
        _jsonOptions = new GaldrJsonOptions()
        {
            PropertyNameCaseInsensitive = false,
            PropertyNamingPolicy = PropertyNamingPolicy.Exact,
            WriteIndented = false,
            DetectCycles = true,
        };
        _ensuredCollections = new HashSet<string>();
        _ddlLock = new object();
        _documentCountLock = new object();
        _commitSerializationLock = new object();
        _secondaryIndexCache = new ConcurrentDictionary<string, SecondaryIndexBTree>();
        _primaryBTreeCache = new ConcurrentDictionary<string, BTree>();
        _pendingRootUpdates = new AsyncLocal<PendingRootUpdates>();
    }

    #endregion

    #region Public Methods

    /// <summary>
    /// Creates a new database file at the specified path.
    /// </summary>
    /// <param name="filePath">Path where the database file will be created.</param>
    /// <param name="options">Configuration options for the database.</param>
    /// <returns>A new GaldrDb instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the file already exists.</exception>
    public static GaldrDb Create(string filePath, GaldrDbOptions options)
    {
        GaldrDb db = new GaldrDb(filePath, options);

        try
        {
            db.ValidateOptions();
            db.InitializeFile();
            db.InitializePools();
            return db;
        }
        catch
        {
            db.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens an existing database file.
    /// </summary>
    /// <param name="filePath">Path to the database file.</param>
    /// <param name="options">Optional configuration options. PageSize is always read from the file.</param>
    /// <returns>A GaldrDb instance for the existing database.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
    /// <exception cref="InvalidPasswordException">Thrown if the database is encrypted and no password is provided, or password is incorrect.</exception>
    /// <exception cref="InvalidOperationException">Thrown if a password is provided for an unencrypted database.</exception>
    public static GaldrDb Open(string filePath, GaldrDbOptions options = null)
    {
        // Always read page size from file header (skip only for custom page IO in simulation testing)
        if (options == null || options.CustomPageIO == null)
        {
            bool isEncrypted = EncryptedPageIO.IsEncryptedFile(filePath);
            int filePageSize;

            if (isEncrypted)
            {
                if (options?.Encryption?.Password == null)
                {
                    throw new InvalidPasswordException("Database is encrypted but no password was provided.");
                }
                filePageSize = EncryptedPageIO.GetPageSize(filePath);
            }
            else
            {
                if (options?.Encryption?.Password != null)
                {
                    throw new InvalidOperationException("Password provided but database is not encrypted.");
                }
                byte[] peekBuffer = new byte[12];
                using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    fs.ReadExactly(peekBuffer, 0, 12);
                }
                filePageSize = BinaryHelper.ReadInt32LE(peekBuffer, 8);
            }

            if (options == null)
            {
                options = new GaldrDbOptions { PageSize = filePageSize };
            }
            else
            {
                options.PageSize = filePageSize;
            }
        }

        GaldrDb db = new GaldrDb(filePath, options);

        try
        {
            db.OpenAndValidateFile();
            db.InitializePools();
            return db;
        }
        catch
        {
            db.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Opens an existing database file, or creates a new one if it doesn't exist.
    /// </summary>
    /// <param name="filePath">Path to the database file.</param>
    /// <param name="options">Optional configuration options for the database.</param>
    /// <returns>A GaldrDb instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if CustomPageIO is set in options.</exception>
    public static GaldrDb OpenOrCreate(string filePath, GaldrDbOptions options = null)
    {
        if (options?.CustomPageIO != null)
        {
            throw new InvalidOperationException("OpenOrCreate does not support CustomPageIO. Use Create or Open directly.");
        }

        GaldrDb db;

        if (File.Exists(filePath))
        {
            db = Open(filePath, options);
        }
        else
        {
            db = Create(filePath, options ?? new GaldrDbOptions());
        }

        return db;
    }

    /// <summary>
    /// Disposes the database, flushing any pending writes and releasing resources.
    /// </summary>
    public void Dispose()
    {
        // Persist header (includes NextFreePageHint) before closing
        if (_pageManager != null)
        {
            _pageManager.PersistHeader();
        }

        // Flush WAL to ensure all committed data is persisted
        if (_wal != null)
        {
            _wal.Flush();
            _wal.Dispose();
            _wal = null;
        }

        if (_documentStorage != null)
        {
            _documentStorage.Dispose();
            _documentStorage = null;
        }

        if (_pageLockManager != null)
        {
            _pageLockManager.Dispose();
            _pageLockManager = null;
        }

        if (_pageCache != null)
        {
            _pageCache.Close();
            _pageCache.Dispose();
            _pageCache = null;
            _basePageIO = null;
        }
        else if (_basePageIO != null)
        {
            _basePageIO.Close();
            _basePageIO.Dispose();
            _basePageIO = null;
        }

        _pageIO = null;
        _walPageIO = null;

        if (_encryptionKey != null)
        {
            System.Security.Cryptography.CryptographicOperations.ZeroMemory(_encryptionKey);
            _encryptionKey = null;
        }
    }

    /// <summary>
    /// Checkpoints the WAL, applying all pending writes to the main database file.
    /// </summary>
    public void Checkpoint()
    {
        if (_walPageIO != null)
        {
            _pageManager.PersistHeader();
            _walPageIO.Checkpoint();
        }
    }

    /// <summary>
    /// Asynchronously checkpoints the WAL, applying all pending writes to the main database file.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (_walPageIO != null)
        {
            _pageManager.PersistHeader();
            await _walPageIO.CheckpointAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Removes old document versions and compacts fragmented pages.
    /// </summary>
    /// <returns>Statistics about the vacuum operation.</returns>
    public GarbageCollectionResult Vacuum()
    {
        GarbageCollectionResult gcResult = CollectGarbageInternal();
        int pagesCompacted = CompactFragmentedPages();

        return new GarbageCollectionResult(
            gcResult.VersionsCollected,
            gcResult.DocumentsProcessed,
            gcResult.CollectableVersions,
            pagesCompacted);
    }

    private GarbageCollectionResult CollectGarbageInternal()
    {
        EnsureGarbageCollector();
        GarbageCollectionResult gcResult = _garbageCollector.Collect();

        if (gcResult.VersionsCollected > 0)
        {
            ulong walTxId = _txManager.AllocateTxId().Value;
            BeginWalTransaction(walTxId);

            try
            {
                _garbageCollector.UnlinkVersions(gcResult.CollectableVersions);

                foreach (CollectableVersion collectable in gcResult.CollectableVersions)
                {
                    // Gracefully handle if slot was already deleted/compacted
                    _documentStorage.TryDeleteDocument(collectable.Location.PageId, collectable.Location.SlotIndex);
                }

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
        }

        return gcResult;
    }

    /// <summary>
    /// Asynchronously removes old document versions and compacts fragmented pages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Statistics about the vacuum operation.</returns>
    public Task<GarbageCollectionResult> VacuumAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(Vacuum());
    }

    /// <summary>
    /// Creates a compacted copy of the database at the target path.
    /// </summary>
    /// <param name="targetPath">Path for the compacted database file.</param>
    /// <returns>Statistics about the compaction.</returns>
    /// <exception cref="InvalidOperationException">Thrown if transactions are active or target file exists.</exception>
    public DatabaseCompactResult CompactTo(string targetPath)
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        if (_txManager.ActiveTransactionCount > 0)
        {
            throw new InvalidOperationException("Cannot compact database while transactions are active");
        }

        if (File.Exists(targetPath))
        {
            throw new InvalidOperationException($"Target file already exists: {targetPath}");
        }
        
        Checkpoint();

        long sourceSize = new FileInfo(_filePath).Length;
        int documentsCopied = 0;
        int collectionsCopied = 0;
        TxId currentSnapshot = _txManager.GetSnapshotTxId();

        using (GaldrDb targetDb = Create(targetPath, _options))
        {
            foreach (IGaldrTypeInfo typeInfo in GaldrTypeRegistry.GetAll())
            {
                if (typeInfo is IGaldrProjectionTypeInfo)
                {
                    continue;
                }

                string collectionName = typeInfo.CollectionName;
                CollectionEntry sourceCollection = _collectionsMetadata.FindCollection(collectionName);
                if (sourceCollection == null)
                {
                    continue;
                }

                List<DocumentVersion> liveVersions = _versionIndex.GetAllVisibleVersions(collectionName, currentSnapshot);
                if (liveVersions == null || liveVersions.Count == 0)
                {
                    continue;
                }

                CollectionEntry targetCollection = targetDb._collectionsMetadata.FindCollection(collectionName);
                if (targetCollection == null)
                {
                    targetDb.CreateCollection(collectionName);
                    targetCollection = targetDb._collectionsMetadata.FindCollection(collectionName);
                }
                targetCollection.NextId = sourceCollection.NextId;

                foreach (IndexDefinition sourceIndex in sourceCollection.Indexes)
                {
                    IndexDefinition newIndex = new IndexDefinition(sourceIndex.Fields, -1, sourceIndex.IsUnique);
                    targetCollection.Indexes.Add(newIndex);
                }

                targetDb._collectionsMetadata.UpdateCollection(targetCollection);
                targetDb.WriteCollectionsMetadataWithGrowth();

                foreach (DocumentVersion version in liveVersions)
                {
                    byte[] docBytes = _documentStorage.ReadDocument(version.Location.PageId, version.Location.SlotIndex);
                    InsertRawForCompaction(targetDb, collectionName, version.DocumentId, docBytes, typeInfo);
                    documentsCopied++;
                }

                collectionsCopied++;
            }

            targetDb.Checkpoint();
        }

        long targetSize = new FileInfo(targetPath).Length;

        return new DatabaseCompactResult(collectionsCopied, documentsCopied, sourceSize, targetSize);
    }

    /// <summary>
    /// Asynchronously creates a compacted copy of the database at the target path.
    /// </summary>
    /// <param name="targetPath">Path for the compacted database file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Statistics about the compaction.</returns>
    /// <exception cref="InvalidOperationException">Thrown if transactions are active or target file exists.</exception>
    public async Task<DatabaseCompactResult> CompactToAsync(string targetPath, CancellationToken cancellationToken = default)
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        if (_txManager.ActiveTransactionCount > 0)
        {
            throw new InvalidOperationException("Cannot compact database while transactions are active");
        }

        if (File.Exists(targetPath))
        {
            throw new InvalidOperationException($"Target file already exists: {targetPath}");
        }

        await CheckpointAsync(cancellationToken).ConfigureAwait(false);

        long sourceSize = new FileInfo(_filePath).Length;
        int documentsCopied = 0;
        int collectionsCopied = 0;
        TxId currentSnapshot = _txManager.GetSnapshotTxId();

        using (GaldrDb targetDb = Create(targetPath, _options))
        {
            foreach (IGaldrTypeInfo typeInfo in GaldrTypeRegistry.GetAll())
            {
                if (typeInfo is IGaldrProjectionTypeInfo)
                {
                    continue;
                }

                string collectionName = typeInfo.CollectionName;
                CollectionEntry sourceCollection = _collectionsMetadata.FindCollection(collectionName);
                if (sourceCollection == null)
                {
                    continue;
                }

                List<DocumentVersion> liveVersions = _versionIndex.GetAllVisibleVersions(collectionName, currentSnapshot);
                if (liveVersions == null || liveVersions.Count == 0)
                {
                    continue;
                }

                CollectionEntry targetCollection = targetDb._collectionsMetadata.FindCollection(collectionName);
                if (targetCollection == null)
                {
                    targetDb.CreateCollection(collectionName);
                    targetCollection = targetDb._collectionsMetadata.FindCollection(collectionName);
                }
                targetCollection.NextId = sourceCollection.NextId;

                foreach (IndexDefinition sourceIndex in sourceCollection.Indexes)
                {
                    IndexDefinition newIndex = new IndexDefinition(sourceIndex.Fields, -1, sourceIndex.IsUnique);
                    targetCollection.Indexes.Add(newIndex);
                }

                targetDb._collectionsMetadata.UpdateCollection(targetCollection);
                targetDb.WriteCollectionsMetadataWithGrowth();

                foreach (DocumentVersion version in liveVersions)
                {
                    byte[] docBytes = await _documentStorage.ReadDocumentAsync(version.Location.PageId, version.Location.SlotIndex, cancellationToken).ConfigureAwait(false);
                    await InsertRawForCompactionAsync(targetDb, collectionName, version.DocumentId, docBytes, typeInfo, cancellationToken).ConfigureAwait(false);
                    documentsCopied++;
                }

                collectionsCopied++;
            }

            await targetDb.CheckpointAsync(cancellationToken).ConfigureAwait(false);
        }

        long targetSize = new FileInfo(targetPath).Length;

        return new DatabaseCompactResult(collectionsCopied, documentsCopied, sourceSize, targetSize);
    }

    /// <summary>
    /// Begins a new read-write transaction with snapshot isolation.
    /// </summary>
    /// <returns>A new transaction that must be committed or disposed.</returns>
    public ITransaction BeginTransaction()
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        _txManager.AllocateAndRegisterTransaction(out TxId txId, out TxId snapshotTxId);
        BeginWalSnapshot(txId.Value);

        Transaction tx = new Transaction(
            this,
            _txManager,
            _versionIndex,
            txId,
            snapshotTxId,
            false,
            _jsonSerializer,
            _jsonOptions);

        return tx;
    }

    /// <summary>
    /// Begins a new read-only transaction with snapshot isolation.
    /// </summary>
    /// <returns>A new read-only transaction that must be disposed when complete.</returns>
    public ITransaction BeginReadOnlyTransaction()
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        _txManager.AllocateAndRegisterTransaction(out TxId txId, out TxId snapshotTxId);
        BeginWalSnapshot(txId.Value);

        Transaction tx = new Transaction(
            this,
            _txManager,
            _versionIndex,
            txId,
            snapshotTxId,
            true,
            _jsonSerializer,
            _jsonOptions);

        return tx;
    }

    internal void CreateCollection(string collectionName)
    {
        CollectionEntry existingCollection = _collectionsMetadata.FindCollection(collectionName);
        if (existingCollection != null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' already exists");
        }

        EnsureTransactionManager();
        ulong walTxId = _txManager.AllocateTxId().Value;
        BeginWalTransaction(walTxId);

        try
        {
            int rootPageId = _pageManager.AllocatePage();

            int order = CalculateBTreeOrder(_options.UsablePageSize);
            BTreeNode rootNode = new BTreeNode(_options.PageSize, order, BTreeNodeType.Leaf);
            byte[] rootBuffer = BufferPool.Rent(_options.PageSize);
            try
            {
                rootNode.SerializeTo(rootBuffer);
                _pageIO.WritePage(rootPageId, rootBuffer);
            }
            finally
            {
                BufferPool.Return(rootBuffer);
            }

            _collectionsMetadata.AddCollection(collectionName, rootPageId);
            WriteCollectionsMetadataWithGrowth();

            _pageManager.SetFreeSpaceLevel(rootPageId, FreeSpaceLevel.None);

            CommitWalTransaction();
        }
        catch
        {
            AbortWalTransaction();
            throw;
        }
    }

    #endregion

    #region Schema Management

    /// <summary>
    /// Gets the names of all collections in the database.
    /// </summary>
    /// <returns>A list of collection names.</returns>
    public IReadOnlyList<string> GetCollectionNames()
    {
        List<CollectionEntry> collections = _collectionsMetadata.GetAllCollections();
        List<string> names = new List<string>(collections.Count);

        for (int i = 0; i < collections.Count; i++)
        {
            names.Add(collections[i].Name);
        }

        return names;
    }

    /// <summary>
    /// Gets the names of all indexes on a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>A list of index field names.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the collection does not exist.</exception>
    public IReadOnlyList<string> GetIndexNames(string collectionName)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
        }

        List<string> names = new List<string>(collection.Indexes.Count);

        for (int i = 0; i < collection.Indexes.Count; i++)
        {
            names.Add(collection.Indexes[i].FieldName);
        }

        return names;
    }

    /// <summary>
    /// Gets detailed information about all indexes on a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>A list of index information.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the collection does not exist.</exception>
    public IReadOnlyList<IndexInfo> GetIndexes(string collectionName)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
        }

        List<IndexInfo> indexes = new List<IndexInfo>(collection.Indexes.Count);

        for (int i = 0; i < collection.Indexes.Count; i++)
        {
            IndexDefinition def = collection.Indexes[i];
            indexes.Add(new IndexInfo(def.FieldName, def.FieldType, def.IsUnique));
        }

        return indexes;
    }

    /// <summary>
    /// Gets detailed information about a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>Collection information including document count and indexes.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the collection does not exist.</exception>
    public CollectionInfo GetCollectionInfo(string collectionName)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
        }

        List<IndexInfo> indexes = new List<IndexInfo>(collection.Indexes.Count);

        for (int i = 0; i < collection.Indexes.Count; i++)
        {
            IndexDefinition def = collection.Indexes[i];
            indexes.Add(new IndexInfo(def.FieldName, def.FieldType, def.IsUnique));
        }

        return new CollectionInfo(collection.Name, collection.DocumentCount, indexes);
    }

    /// <summary>
    /// Drops an index from a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="fieldName">The indexed field name.</param>
    /// <exception cref="InvalidOperationException">Thrown if the collection or index does not exist.</exception>
    public void DropIndex(string collectionName, string fieldName)
    {
        lock (_ddlLock)
        {
            CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
            if (collection == null)
            {
                throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
            }

            IndexDefinition indexToRemove = null;
            int indexPosition = -1;

            for (int i = 0; i < collection.Indexes.Count; i++)
            {
                if (collection.Indexes[i].FieldName == fieldName)
                {
                    indexToRemove = collection.Indexes[i];
                    indexPosition = i;
                    break;
                }
            }

            if (indexToRemove == null)
            {
                throw new InvalidOperationException($"Index '{fieldName}' does not exist on collection '{collectionName}'.");
            }

            EnsureTransactionManager();
            ulong walTxId = _txManager.AllocateTxId().Value;
            BeginWalTransaction(walTxId);

            try
            {
                SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexToRemove);

                List<int> pageIds = indexTree.CollectAllPageIds();
                for (int i = 0; i < pageIds.Count; i++)
                {
                    _pageManager.DeallocatePage(pageIds[i]);
                }

                // Remove from cache before removing from collection
                _secondaryIndexCache.TryRemove($"{collectionName}:{indexToRemove.IndexName}", out _);

                collection.Indexes.RemoveAt(indexPosition);
                _collectionsMetadata.UpdateCollection(collection);
                WriteCollectionsMetadataWithGrowth();

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
        }
    }

    /// <summary>
    /// Drops a collection and optionally deletes all its documents.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="deleteDocuments">If true, deletes all documents. If false and documents exist, throws.</param>
    /// <exception cref="InvalidOperationException">Thrown if collection doesn't exist or has documents and deleteDocuments is false.</exception>
    public void DropCollection(string collectionName, bool deleteDocuments = false)
    {
        lock (_ddlLock)
        {
            CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
            if (collection == null)
            {
                throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
            }

            if (collection.DocumentCount > 0 && !deleteDocuments)
            {
                throw new InvalidOperationException($"Collection '{collectionName}' contains {collection.DocumentCount} document(s). Set deleteDocuments to true to delete the collection and all its documents.");
            }

            EnsureTransactionManager();
            ulong walTxId = _txManager.AllocateTxId().Value;
            BeginWalTransaction(walTxId);

            try
            {
                for (int i = 0; i < collection.Indexes.Count; i++)
                {
                    IndexDefinition index = collection.Indexes[i];
                    SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, index);

                    List<int> indexPageIds = indexTree.CollectAllPageIds();
                    for (int j = 0; j < indexPageIds.Count; j++)
                    {
                        _pageManager.DeallocatePage(indexPageIds[j]);
                    }

                    // Remove from cache
                    _secondaryIndexCache.TryRemove($"{collectionName}:{index.IndexName}", out _);
                }

                BTree primaryTree = GetPrimaryBTree(collection);

                if (deleteDocuments)
                {
                    List<BTreeEntry> allEntries = primaryTree.GetAllEntries();
                    for (int i = 0; i < allEntries.Count; i++)
                    {
                        DocumentLocation location = allEntries[i].Location;
                        _documentStorage.DeleteDocument(location.PageId, location.SlotIndex);
                    }
                }

                List<int> treePageIds = primaryTree.CollectAllPageIds();
                for (int i = 0; i < treePageIds.Count; i++)
                {
                    _pageManager.DeallocatePage(treePageIds[i]);
                }

                _collectionsMetadata.RemoveCollection(collectionName);
                WriteCollectionsMetadataWithGrowth();

                _ensuredCollections.Remove(collectionName);
                _primaryBTreeCache.TryRemove(collectionName, out _);

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
        }
    }

    /// <summary>
    /// Detects collections and indexes in the database that are not registered with any type.
    /// </summary>
    /// <returns>Information about orphaned collections and indexes.</returns>
    public OrphanedSchemaInfo GetOrphanedSchema()
    {
        List<string> orphanedCollections = new List<string>();
        List<OrphanedIndexInfo> orphanedIndexes = new List<OrphanedIndexInfo>();

        HashSet<string> registeredCollectionNames = new HashSet<string>();
        Dictionary<string, HashSet<string>> registeredIndexesByCollection = new Dictionary<string, HashSet<string>>();

        if (GaldrTypeRegistry.IsInitialized)
        {
            foreach (IGaldrTypeInfo typeInfo in GaldrTypeRegistry.GetAll())
            {
                if (typeInfo is IGaldrProjectionTypeInfo)
                {
                    continue;
                }

                registeredCollectionNames.Add(typeInfo.CollectionName);

                HashSet<string> indexedFields = new HashSet<string>();
                for (int i = 0; i < typeInfo.IndexedFieldNames.Count; i++)
                {
                    indexedFields.Add(typeInfo.IndexedFieldNames[i]);
                }
                for (int i = 0; i < typeInfo.CompoundIndexes.Count; i++)
                {
                    indexedFields.Add(typeInfo.CompoundIndexes[i].IndexName);
                }
                registeredIndexesByCollection[typeInfo.CollectionName] = indexedFields;
            }
        }

        List<CollectionEntry> allCollections = _collectionsMetadata.GetAllCollections();

        for (int i = 0; i < allCollections.Count; i++)
        {
            CollectionEntry collection = allCollections[i];

            if (!registeredCollectionNames.Contains(collection.Name))
            {
                orphanedCollections.Add(collection.Name);
            }
            else
            {
                HashSet<string> registeredIndexes = registeredIndexesByCollection[collection.Name];

                for (int j = 0; j < collection.Indexes.Count; j++)
                {
                    IndexDefinition index = collection.Indexes[j];
                    string indexKey = index.IsCompound ? index.IndexName : index.FieldName;
                    if (!registeredIndexes.Contains(indexKey))
                    {
                        orphanedIndexes.Add(new OrphanedIndexInfo(collection.Name, indexKey));
                    }
                }
            }
        }

        return new OrphanedSchemaInfo(orphanedCollections, orphanedIndexes);
    }

    /// <summary>
    /// Removes orphaned collections and indexes from the database.
    /// </summary>
    /// <param name="deleteDocuments">If true, deletes documents in orphaned collections.</param>
    /// <returns>Information about what was cleaned up.</returns>
    public OrphanedSchemaInfo CleanupOrphanedSchema(bool deleteDocuments = false)
    {
        OrphanedSchemaInfo orphans = GetOrphanedSchema();

        for (int i = 0; i < orphans.Collections.Count; i++)
        {
            DropCollection(orphans.Collections[i], deleteDocuments);
        }

        for (int i = 0; i < orphans.Indexes.Count; i++)
        {
            OrphanedIndexInfo index = orphans.Indexes[i];
            DropIndex(index.CollectionName, index.FieldName);
        }

        return orphans;
    }

    #endregion

    #region Type-Safe CRUD Operations

    /// <summary>
    /// Inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    public int Insert<T>(T document)
    {
        using (ITransaction tx = BeginTransaction())
        {
            int id = tx.Insert<T>(document);
            tx.Commit();
            return id;
        }
    }

    /// <summary>
    /// Gets a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>The document, or default if not found.</returns>
    public T GetById<T>(int id)
    {
        using (ITransaction tx = BeginReadOnlyTransaction())
        {
            T result = tx.GetById<T>(id);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    public bool Replace<T>(T document)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = tx.Replace<T>(document);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Creates a partial update builder for updating specific fields of a document by ID.
    /// The update is executed in an auto-committed transaction when Execute() is called.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>An UpdateBuilder for chaining Set calls.</returns>
    public IUpdateBuilder<T> UpdateById<T>(int id)
    {
        GaldrTypeInfo<T> typeInfo = GaldrTypeRegistry.Get<T>();
        return new UpdateBuilder<T>(this, typeInfo, id);
    }

    /// <summary>
    /// Returns a builder for performing a partial update on a document by ID using runtime field names.
    /// The update is executed in an auto-committed transaction when Execute() is called.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>A DynamicUpdateBuilder for chaining Set calls.</returns>
    public IDynamicUpdateBuilder UpdateByIdDynamic(string collectionName, int id)
    {
        return new DynamicUpdateBuilder(this, collectionName, id);
    }

    /// <summary>
    /// Deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    public bool DeleteById<T>(int id)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = tx.DeleteById<T>(id);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Creates a query builder for the specified document type.
    /// </summary>
    /// <typeparam name="T">The document type to query.</typeparam>
    /// <returns>A query builder for constructing and executing queries.</returns>
    public QueryBuilder<T> Query<T>()
    {
        ITransaction tx = BeginReadOnlyTransaction();
        QueryBuilder<T> innerQuery = tx.Query<T>();

        // Wrap the executor to auto-dispose the transaction after query execution
        AutoDisposingQueryExecutor<T> autoDisposingExecutor = new AutoDisposingQueryExecutor<T>(
            innerQuery.GetExecutor(),
            tx);

        return new QueryBuilder<T>(autoDisposingExecutor);
    }

    internal CollectionEntry GetCollection(string collectionName)
    {
        return _collectionsMetadata.FindCollection(collectionName);
    }

    #endregion

    #region Async Type-Safe CRUD Operations

    /// <summary>
    /// Asynchronously inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
    public async Task<int> InsertAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginTransaction())
        {
            int id = await tx.InsertAsync<T>(document, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return id;
        }
    }

    /// <summary>
    /// Asynchronously gets a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or default if not found.</returns>
    public async Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginReadOnlyTransaction())
        {
            T result = await tx.GetByIdAsync<T>(id, cancellationToken).ConfigureAwait(false);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Asynchronously updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    public async Task<bool> ReplaceAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = await tx.ReplaceAsync<T>(document, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
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
        using (ITransaction tx = BeginTransaction())
        {
            bool result = await tx.DeleteByIdAsync<T>(id, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
    }

    #endregion

    #region Dynamic CRUD Operations

    /// <summary>
    /// Gets a document by ID as a JsonDocument.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    public JsonDocument GetByIdDynamic(string collectionName, int id)
    {
        using (ITransaction tx = BeginReadOnlyTransaction())
        {
            JsonDocument result = tx.GetByIdDynamic(collectionName, id);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Asynchronously gets a document by ID as a JsonDocument.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    public async Task<JsonDocument> GetByIdDynamicAsync(string collectionName, int id, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginReadOnlyTransaction())
        {
            JsonDocument result = await tx.GetByIdDynamicAsync(collectionName, id, cancellationToken).ConfigureAwait(false);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Creates a dynamic query builder for the specified collection.
    /// </summary>
    /// <param name="collectionName">The collection name to query.</param>
    /// <returns>A dynamic query builder for constructing and executing queries.</returns>
    public DynamicQueryBuilder QueryDynamic(string collectionName)
    {
        ITransaction tx = BeginReadOnlyTransaction();
        DynamicQueryBuilder innerQuery = tx.QueryDynamic(collectionName);

        AutoDisposingDynamicQueryExecutor autoDisposingExecutor = new AutoDisposingDynamicQueryExecutor(
            innerQuery.GetExecutor(),
            tx);

        CollectionEntry collection = GetCollection(collectionName);
        return new DynamicQueryBuilder(collectionName, collection, autoDisposingExecutor);
    }

    /// <summary>
    /// Inserts a JSON document and returns its assigned ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="json">The JSON document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    public int InsertDynamic(string collectionName, string json)
    {
        using (ITransaction tx = BeginTransaction())
        {
            int id = tx.InsertDynamic(collectionName, json);
            tx.Commit();
            return id;
        }
    }

    /// <summary>
    /// Asynchronously inserts a JSON document and returns its assigned ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="json">The JSON document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
    public async Task<int> InsertDynamicAsync(string collectionName, string json, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginTransaction())
        {
            int id = await tx.InsertDynamicAsync(collectionName, json, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return id;
        }
    }

    /// <summary>
    /// Replaces an existing JSON document.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="json">The JSON document with updated values.</param>
    /// <returns>True if the document was found and replaced, false otherwise.</returns>
    public bool ReplaceDynamic(string collectionName, int id, string json)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = tx.ReplaceDynamic(collectionName, id, json);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Asynchronously replaces an existing JSON document.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="json">The JSON document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and replaced, false otherwise.</returns>
    public async Task<bool> ReplaceDynamicAsync(string collectionName, int id, string json, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = await tx.ReplaceDynamicAsync(collectionName, id, json, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
    }

    /// <summary>
    /// Deletes a document by its ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    public bool DeleteByIdDynamic(string collectionName, int id)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = tx.DeleteByIdDynamic(collectionName, id);
            tx.Commit();
            return result;
        }
    }

    /// <summary>
    /// Asynchronously deletes a document by its ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    public async Task<bool> DeleteByIdDynamicAsync(string collectionName, int id, CancellationToken cancellationToken = default)
    {
        using (ITransaction tx = BeginTransaction())
        {
            bool result = await tx.DeleteByIdDynamicAsync(collectionName, id, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
    }

    #endregion

    #region Index Operations

    private void EnsureIndexes(CollectionEntry collection, IGaldrTypeInfo typeInfo)
    {
        IReadOnlyList<string> indexedFieldNames = typeInfo.IndexedFieldNames;
        IReadOnlyList<string> uniqueIndexFieldNames = typeInfo.UniqueIndexFieldNames;
        IReadOnlyList<CompoundIndexInfo> compoundIndexes = typeInfo.CompoundIndexes;
        List<IndexDefinition> newIndexes = new List<IndexDefinition>();

        foreach (string fieldName in indexedFieldNames)
        {
            IndexDefinition existingIndex = collection.FindIndex(fieldName);
            if (existingIndex == null)
            {
                bool isUnique = ContainsFieldName(uniqueIndexFieldNames, fieldName);

                IndexDefinition newIndex = new IndexDefinition(fieldName, GaldrFieldType.String, -1, isUnique);
                newIndexes.Add(newIndex);
            }
        }

        foreach (CompoundIndexInfo compoundInfo in compoundIndexes)
        {
            IndexDefinition existingIndex = collection.FindIndexByName(compoundInfo.IndexName);
            if (existingIndex == null)
            {
                List<IndexField> fields = new List<IndexField>(compoundInfo.Fields.Count);
                foreach (CompoundIndexField field in compoundInfo.Fields)
                {
                    fields.Add(new IndexField(field.FieldName, field.FieldType));
                }

                IndexDefinition newIndex = new IndexDefinition(fields, -1, compoundInfo.IsUnique);
                newIndexes.Add(newIndex);
            }
        }

        if (newIndexes.Count > 0)
        {
            EnsureTransactionManager();
            ulong walTxId = _txManager.AllocateTxId().Value;
            BeginWalTransaction(walTxId);

            byte[] rootBuffer = BufferPool.Rent(_options.PageSize);
            try
            {
                foreach (IndexDefinition newIndex in newIndexes)
                {
                    int rootPageId = _pageManager.AllocatePage();
                    int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.UsablePageSize);
                    SecondaryIndexNode rootNode = new SecondaryIndexNode(_options.UsablePageSize, maxKeys, BTreeNodeType.Leaf);

                    rootNode.SerializeTo(rootBuffer);
                    _pageIO.WritePage(rootPageId, rootBuffer);

                    newIndex.RootPageId = rootPageId;
                    collection.Indexes.Add(newIndex);
                }

                _collectionsMetadata.UpdateCollection(collection);
                WriteCollectionsMetadataWithGrowth();

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
            finally
            {
                BufferPool.Return(rootBuffer);
            }
        }
    }
    
    internal void AddIndexForTesting(string collectionName, string fieldName)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist.");
        }

        if (collection.FindIndex(fieldName) != null)
        {
            throw new InvalidOperationException($"Index '{fieldName}' already exists on collection '{collectionName}'.");
        }

        EnsureTransactionManager();
        ulong walTxId = _txManager.AllocateTxId().Value;
        BeginWalTransaction(walTxId);

        byte[] rootBuffer = BufferPool.Rent(_options.PageSize);
        try
        {
            int rootPageId = _pageManager.AllocatePage();
            int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.UsablePageSize);
            SecondaryIndexNode rootNode = new SecondaryIndexNode(_options.UsablePageSize, maxKeys, BTreeNodeType.Leaf);

            rootNode.SerializeTo(rootBuffer);
            _pageIO.WritePage(rootPageId, rootBuffer);

            IndexDefinition newIndex = new IndexDefinition(fieldName, GaldrFieldType.String, rootPageId, false);
            collection.Indexes.Add(newIndex);
            _collectionsMetadata.UpdateCollection(collection);
            WriteCollectionsMetadataWithGrowth();

            CommitWalTransaction();
        }
        catch
        {
            AbortWalTransaction();
            throw;
        }
        finally
        {
            BufferPool.Return(rootBuffer);
        }
    }

    private void CheckUniqueConstraint(string collectionName, IndexDefinition indexDef, string fieldName, byte[] keyBytes)
    {
        // Skip unique constraint check for null values (NULL != NULL in database semantics)
        if (!(keyBytes.Length == 1 && keyBytes[0] == 0x00))
        {
            SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexDef);
            List<DocumentLocation> existingEntries = indexTree.SearchByFieldValue(keyBytes);
            if (existingEntries.Count > 0)
            {
                throw new InvalidOperationException($"Unique constraint violation: A document with {fieldName} = '{GetFieldValueString(keyBytes)}' already exists.");
            }
        }
    }

    private static string GetFieldValueString(byte[] keyBytes)
    {
        string result;

        try
        {
            result = Encoding.UTF8.GetString(keyBytes);
        }
        catch
        {
            result = BitConverter.ToString(keyBytes);
        }

        return result;
    }

    #endregion

    #region Private Methods

    private void ValidateOptions()
    {
        if (_options.PageSize < 1024)
        {
            throw new ArgumentException("PageSize must be at least 1024 bytes");
        }

        int pageSize = _options.PageSize;
        bool isPowerOfTwo = (pageSize & (pageSize - 1)) == 0;

        if (!isPowerOfTwo)
        {
            throw new ArgumentException("PageSize must be a power of 2");
        }
    }

    private void InitializePools()
    {
        JsonWriterPool.Configure(_options.JsonWriterBufferSize);

        if (_options.WarmupOnOpen)
        {
            JsonWriterPool.Warmup(_options.JsonWriterPoolWarmupCount);

            int order = CalculateBTreeOrder(_options.UsablePageSize);
            int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.UsablePageSize);

            BTreeNodePool.Warmup(_options.PageSize, order, 8);
            SecondaryIndexNodePool.Warmup(_options.UsablePageSize, maxKeys, 8);
            DocumentPagePool.Warmup(_options.PageSize, 8);
        }
    }

    private void InitializeFile()
    {
        if (File.Exists(_filePath))
        {
            throw new InvalidOperationException("File exists; use Open.");
        }

        SetupIO();
        
        // always initialize the file with a valid header
        _pageManager = new PageManager(_basePageIO, _options.PageSize, _options.ExpansionPageCount, _options.UsablePageSize);
        _pageManager.Initialize();
        _pageManager.Flush();

        if (_options.UseWal)
        {
            InitializeWalFile();
        }
        
        // future page writes should go through WAL, if configured
        _pageManager.SetPageIO(_pageIO);
        
        _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataStartPage, _pageManager.Header.CollectionsMetadataPageCount, _options.PageSize, _options.UsablePageSize);
        WriteCollectionsMetadataWithGrowth();

        _pageLockManager = new PageLockManager();
        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageLockManager, _options.PageSize, _options.UsablePageSize);

        EnsureAllCollections();
    }

    private void OpenAndValidateFile()
    {
        // Skip file existence check when using custom page IO (simulation testing)
        if (_options.CustomPageIO == null && !File.Exists(_filePath))
        {
            throw new FileNotFoundException();
        }

        SetupIO();
        
        if (_options.UseWal)
        {
            // When using custom WAL stream, always recover (stream contains WAL data)
            // When using file-based WAL, check if file exists
            bool walExists = _options.CustomWalStreamIO != null || File.Exists(_walPath);

            if (walExists)
            {
                RecoverFromWal();
            }
            else
            {
                InitializeWalFile();

                _pageManager = new PageManager(_pageIO, _options.PageSize, _options.ExpansionPageCount, _options.UsablePageSize);
                _pageManager.Load();

                if (_options.PageSize == 0 || _options.PageSize != _pageManager.Header.PageSize)
                {
                    _options.PageSize = _pageManager.Header.PageSize;
                }

                _pageLockManager = new PageLockManager();
                _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageLockManager, _pageManager.Header.PageSize, _options.UsablePageSize);

                _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataStartPage, _pageManager.Header.CollectionsMetadataPageCount, _pageManager.Header.PageSize, _options.UsablePageSize);
                _collectionsMetadata.LoadFromDisk();

                // Rebuild VersionIndex from current database state
                RebuildVersionIndex(0);
            }
        }
        else
        {
            _pageManager = new PageManager(_pageIO, _options.PageSize, _options.ExpansionPageCount, _options.UsablePageSize);
            _pageManager.Load();

            if (_options.PageSize == 0 || _options.PageSize != _pageManager.Header.PageSize)
            {
                _options.PageSize = _pageManager.Header.PageSize;
            }

            _pageLockManager = new PageLockManager();
            _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageLockManager, _pageManager.Header.PageSize, _options.UsablePageSize);

            _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataStartPage, _pageManager.Header.CollectionsMetadataPageCount, _pageManager.Header.PageSize, _options.UsablePageSize);
            _collectionsMetadata.LoadFromDisk();

            // No WAL - still need to rebuild VersionIndex for MVCC reads to work
            RebuildVersionIndex(0);
        }

        EnsureAllCollections();
    }

    private void SetupIO()
    {
        if (_options.CustomPageIO != null)
        {
            _basePageIO = _options.CustomPageIO;
        }
        else if (_options.Encryption?.Password != null)
        {
            bool createNew = !File.Exists(_filePath);
            EncryptedPageIO encryptedIO;

            if (createNew)
            {
                encryptedIO = EncryptedPageIO.Create(_filePath, _options.PageSize, _options.Encryption, out _encryptionKey);
            }
            else
            {
                encryptedIO = EncryptedPageIO.Open(_filePath, _options.PageSize, _options.Encryption, out _encryptionKey);
            }

            _basePageIO = encryptedIO;
        }
        else
        {
            bool useMmap = _options.UseMmap;

            if (useMmap && !MmapPageIO.IsMmapSupported())
            {
                useMmap = false;
            }

            bool createNew = !File.Exists(_filePath);
            long initialSize = (long)_options.PageSize * 4;

            if (useMmap)
            {
                _basePageIO = new MmapPageIO(_filePath, _options.PageSize, initialSize, createNew);
            }
            else
            {
                _basePageIO = new StandardPageIO(_filePath, _options.PageSize, createNew);
            }
        }

        if (_options.PageCacheSize > 0)
        {
            _pageCache = new LruPageCache(_basePageIO, _options.PageSize, _options.PageCacheSize);
            _pageIO = _pageCache;
        }
        else
        {
            _pageIO = _basePageIO;
        }
    }

    private void InitializeWalFile()
    {
        _wal = new WriteAheadLog(_walPath, _options.PageSize);

        if (_options.CustomWalStreamIO != null)
        {
            _wal._testStreamIO = _options.CustomWalStreamIO;
        }
        if (_options.CustomWalSaltGenerator != null)
        {
            _wal._testSaltGenerator = _options.CustomWalSaltGenerator;
        }
        if (_encryptionKey != null)
        {
            _wal._encryptionKey = _encryptionKey;
        }

        _wal.Create();

        IPageIO innerIO = _pageCache ?? _basePageIO;
        _walPageIO = new WalPageIO(innerIO, _wal, _options.PageSize);
        _pageIO = _walPageIO;
    }

    private void RecoverFromWal()
    {
        _wal = new WriteAheadLog(_walPath, _options.PageSize);

        if (_options.CustomWalStreamIO != null)
        {
            _wal._testStreamIO = _options.CustomWalStreamIO;
        }
        if (_options.CustomWalSaltGenerator != null)
        {
            _wal._testSaltGenerator = _options.CustomWalSaltGenerator;
        }
        if (_encryptionKey != null)
        {
            _wal._encryptionKey = _encryptionKey;
        }

        _wal.Open();

        // Get all committed transactions and all frames
        HashSet<ulong> committedTxIds = _wal.GetCommittedTransactions();
        List<WalFrame> allFrames = _wal.ReadAllFrames();

        // Find the highest committed TxId for TransactionManager initialization
        ulong maxCommittedTxId = 0;
        foreach (ulong txId in committedTxIds)
        {
            if (txId > maxCommittedTxId)
            {
                maxCommittedTxId = txId;
            }
        }

        // Apply frames in WAL order (frame number order), but only for committed transactions.
        // This is critical because auto-commits (txId=0) may be interleaved with regular
        // transactions, and we must preserve the actual write order to get correct state.
        foreach (WalFrame frame in allFrames)
        {
            if (committedTxIds.Contains(frame.TxId))
            {
                if (frame.PageId >= 0 && frame.Data.Length > 0)
                {
                    _basePageIO.WritePage(frame.PageId, frame.Data);
                }
            }
        }

        _basePageIO.Flush();
        
        // Truncate WAL after successful recovery
        _wal.Truncate();

        // Set up WAL page IO for future operations
        IPageIO innerIO = _pageCache ?? _basePageIO;
        _walPageIO = new WalPageIO(innerIO, _wal, _options.PageSize);
        _pageIO = _walPageIO;
        
        _pageManager = new PageManager(_pageIO, _options.PageSize, _options.ExpansionPageCount, _options.UsablePageSize);
        _pageManager.Load();

        if (_options.PageSize == 0 || _options.PageSize != _pageManager.Header.PageSize)
        {
            _options.PageSize = _pageManager.Header.PageSize;
        }

        _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataStartPage, _pageManager.Header.CollectionsMetadataPageCount, _pageManager.Header.PageSize, _options.UsablePageSize);
        _collectionsMetadata.LoadFromDisk();

        _pageLockManager = new PageLockManager();
        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageLockManager, _pageManager.Header.PageSize, _options.UsablePageSize);

        // Initialize TransactionManager with recovered TxId
        EnsureTransactionManager();
        if (maxCommittedTxId > 0)
        {
            _txManager.SetLastCommittedTxId(new TxId(maxCommittedTxId));
        }

        // Rebuild VersionIndex from current database state
        RebuildVersionIndex(maxCommittedTxId);
    }

    private void RebuildVersionIndex(ulong recoveredTxId)
    {
        EnsureVersionIndex();

        // Use TxId 0 for recovered documents (they existed before current session)
        // This ensures all new transactions will see them in their snapshot
        TxId baseTxId = new TxId(recoveredTxId > 0 ? recoveredTxId : 0);

        List<CollectionEntry> collections = _collectionsMetadata.GetAllCollections();

        foreach (CollectionEntry collection in collections)
        {
            _versionIndex.EnsureCollection(collection.Name);

            BTree btree = GetPrimaryBTree(collection);
            List<BTreeEntry> entries = btree.GetAllEntries();

            foreach (BTreeEntry entry in entries)
            {
                _versionIndex.AddVersion(collection.Name, entry.Key, baseTxId, entry.Location);
            }
        }
    }

    private int CalculateBTreeOrder(int pageSize)
    {
        const int HeaderSize = 8;
        const int KeySize = 4;
        const int ValueSize = 8;

        int usableSpace = pageSize - HeaderSize;
        int order = (usableSpace / (KeySize + ValueSize)) + 1;

        if (order < 3)
        {
            order = 3;
        }

        return order;
    }

    private static bool ContainsFieldName(IReadOnlyList<string> list, string fieldName)
    {
        bool result = false;

        for (int i = 0; i < list.Count; i++)
        {
            if (list[i] == fieldName)
            {
                result = true;
                break;
            }
        }

        return result;
    }

    private void EnsureTransactionManager()
    {
        if (_txManager == null)
        {
            _txManager = new TransactionManager();
        }
    }

    private void EnsureVersionIndex()
    {
        if (_versionIndex == null)
        {
            _versionIndex = new VersionIndex();
        }
    }

    private void EnsureGarbageCollector()
    {
        if (_garbageCollector == null)
        {
            EnsureTransactionManager();
            EnsureVersionIndex();
            _garbageCollector = new VersionGarbageCollector(_versionIndex, _txManager);
            _lastGCCommitCount = 0;
        }
    }

    internal void WriteCollectionsMetadataWithGrowth()
    {
        int pagesNeeded = _collectionsMetadata.GetPagesNeeded();
        if (pagesNeeded > _collectionsMetadata.GetCurrentPageCount())
        {
            int additional = pagesNeeded - _collectionsMetadata.GetCurrentPageCount();
            _pageManager.GrowCollectionsMetadata(_collectionsMetadata, additional);
        }
        _collectionsMetadata.WriteToDisk();
    }

    private void EnsureAllCollections()
    {
        if (!GaldrTypeRegistry.IsInitialized)
        {
            return;
        }

        lock (_ddlLock)
        {
            foreach (IGaldrTypeInfo typeInfo in GaldrTypeRegistry.GetAll())
            {
                string collectionName = typeInfo.CollectionName;

                if (_ensuredCollections.Contains(collectionName))
                {
                    continue;
                }

                CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);

                if (collection == null)
                {
                    CreateCollection(collectionName);
                    collection = _collectionsMetadata.FindCollection(collectionName);
                }

                EnsureIndexes(collection, typeInfo);
                _ensuredCollections.Add(collectionName);
            }
        }
    }

    internal void TryRunGarbageCollection()
    {
        if (_options.AutoGarbageCollection)
        {
            long currentCommitCount = _txManager.CommitCount;
            long commitsSinceLastGC = currentCommitCount - _lastGCCommitCount;

            if (commitsSinceLastGC >= _options.GarbageCollectionThreshold)
            {
                try
                {
                    CollectGarbageInternal();
                    _lastGCCommitCount = currentCommitCount;
                }
                catch (PageConflictException)
                {
                    // GC encountered a page conflict - skip this run and try again later
                }
            }
        }
    }

    internal void TryRunAutoCheckpoint()
    {
        if (!_options.AutoCheckpoint || _wal == null)
        {
            return;
        }

        if (_wal.CurrentFrameNumber >= _options.WalCheckpointThreshold)
        {
            Checkpoint();
        }
    }

    private int CompactFragmentedPages()
    {
        int compacted = 0;
        int totalPages = _pageManager.Header.TotalPageCount;
        byte[] pageBuffer = BufferPool.Rent(_options.PageSize);
        DocumentPage reusablePage = new DocumentPage();

        try
        {
            for (int pageId = PageConstants.FIRST_DATA_PAGE_ID; pageId < totalPages; pageId++)
            {
                if (!_pageManager.IsAllocated(pageId))
                {
                    continue;
                }

                _pageIO.ReadPage(pageId, pageBuffer);

                if (pageBuffer[0] != PageConstants.PAGE_TYPE_DOCUMENT)
                {
                    continue;
                }

                DocumentPage.DeserializeTo(pageBuffer, reusablePage, _options.PageSize);

                if (reusablePage.NeedsCompaction())
                {
                    reusablePage.Compact();
                    reusablePage.SerializeTo(pageBuffer);
                    _pageIO.WritePage(pageId, pageBuffer);

                    int freeSpaceBytes = reusablePage.GetFreeSpaceBytes();
                    FreeSpaceLevel level = CalculateFreeSpaceLevel(freeSpaceBytes);
                    _pageManager.SetFreeSpaceLevel(pageId, level);

                    compacted++;
                }
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
        }

        return compacted;
    }

    private FreeSpaceLevel CalculateFreeSpaceLevel(int freeSpaceBytes)
    {
        double freeSpacePercentage = (double)freeSpaceBytes / _options.PageSize;
        FreeSpaceLevel level = FreeSpaceLevel.None;

        if (freeSpacePercentage >= 0.70)
        {
            level = FreeSpaceLevel.High;
        }
        else if (freeSpacePercentage >= 0.40)
        {
            level = FreeSpaceLevel.Medium;
        }
        else if (freeSpacePercentage >= 0.10)
        {
            level = FreeSpaceLevel.Low;
        }

        return level;
    }

    // Split WAL lifecycle methods for Transaction use:
    // Snapshot phase: BeginWalSnapshot / EndWalSnapshot
    // Write phase: BeginWalWrite / CommitWalWrite / AbortWalWrite

    internal void BeginWalSnapshot(ulong txId)
    {
        if (_walPageIO != null)
        {
            _walPageIO.BeginSnapshot(txId);
        }
    }

    internal void EndWalSnapshot()
    {
        if (_walPageIO != null)
        {
            _walPageIO.EndSnapshot();
        }
    }

    internal void BeginWalWrite()
    {
        InitPendingRootUpdates();
        if (_walPageIO != null)
        {
            _walPageIO.BeginWrite();
        }
    }

    internal void CommitWalWrite()
    {
        if (_walPageIO != null)
        {
            _walPageIO.CommitWrite();
        }
        ApplyPendingRootUpdates();
    }

    internal void AbortWalWrite()
    {
        if (_walPageIO != null)
        {
            _walPageIO.AbortWrite();
        }
        ClearPendingRootUpdates();
    }

    /// <summary>
    /// Atomically validates version conflicts, commits WAL frames, and adds version entries.
    /// The serialization lock ensures no other transaction can commit between validation and
    /// version addition, preventing both split-validation bugs and TOCTOU races.
    /// Does not end the WAL snapshot — the caller is responsible for that.
    /// </summary>
    internal void CommitWalWriteWithVersions(TxId txId, TxId snapshotTxId, IReadOnlyList<VersionOperation> versionOps)
    {
        lock (_commitSerializationLock)
        {
            _versionIndex.ValidateVersions(txId, snapshotTxId, versionOps);
            CommitWalWrite();
            _versionIndex.AddVersions(txId, versionOps);
        }
    }

    // Convenience methods for internal operations (schema changes, etc.)
    // that need the full snapshot + write lifecycle in one call.

    internal void BeginWalTransaction(ulong txId)
    {
        BeginWalSnapshot(txId);
        BeginWalWrite();
    }

    internal void CommitWalTransaction()
    {
        CommitWalWrite();
        EndWalSnapshot();
    }

    internal void AbortWalTransaction()
    {
        AbortWalWrite();
        EndWalSnapshot();
    }

    private void InitPendingRootUpdates()
    {
        _pendingRootUpdates.Value = new PendingRootUpdates();
    }

    private void ClearPendingRootUpdates()
    {
        _pendingRootUpdates.Value = null;
    }

    internal BTree GetPrimaryBTree(CollectionEntry collection)
    {
        return _primaryBTreeCache.GetOrAdd(collection.Name, _ =>
            new BTree(_pageIO, _pageManager, _pageLockManager, collection.RootPage, _options.PageSize, CalculateBTreeOrder(_options.UsablePageSize)));
    }

    internal SecondaryIndexBTree GetSecondaryIndexBTree(string collectionName, IndexDefinition indexDef)
    {
        string cacheKey = $"{collectionName}:{indexDef.IndexName}";
        return _secondaryIndexCache.GetOrAdd(cacheKey, _ =>
            new SecondaryIndexBTree(_pageIO, _pageManager, _pageLockManager, indexDef.RootPageId, _options.PageSize, _options.UsablePageSize, SecondaryIndexBTree.CalculateMaxKeys(_options.UsablePageSize)));
    }

    private void SetPendingCollectionRoot(CollectionEntry collection, int newRootPageId)
    {
        PendingRootUpdates pending = _pendingRootUpdates.Value;
        if (pending != null)
        {
            pending.SetCollectionRoot(collection.Name, newRootPageId);
        }
        else
        {
            // No transaction context - update directly (for non-transactional operations)
            collection.RootPage = newRootPageId;
        }
    }

    private int GetEffectiveIndexRoot(CollectionEntry collection, IndexDefinition indexDef)
    {
        int result = indexDef.RootPageId;
        PendingRootUpdates pending = _pendingRootUpdates.Value;
        if (pending != null && pending.TryGetIndexRoot(collection.Name, indexDef.IndexName, out int pendingRoot))
        {
            result = pendingRoot;
        }
        return result;
    }

    private void SetPendingIndexRoot(CollectionEntry collection, IndexDefinition indexDef, int newRootPageId)
    {
        PendingRootUpdates pending = _pendingRootUpdates.Value;
        if (pending != null)
        {
            pending.SetIndexRoot(collection.Name, indexDef.IndexName, newRootPageId);
        }
        else
        {
            // No transaction context - update directly (for non-transactional operations)
            indexDef.RootPageId = newRootPageId;
        }
    }

    private void ApplyPendingRootUpdates()
    {
        PendingRootUpdates pending = _pendingRootUpdates.Value;
        if (pending != null)
        {
            // Apply collection root updates
            foreach (KeyValuePair<string, int> kvp in pending.CollectionRoots)
            {
                CollectionEntry collection = _collectionsMetadata.FindCollection(kvp.Key);
                if (collection != null)
                {
                    collection.RootPage = kvp.Value;
                }
            }

            // Apply index root updates
            foreach (KeyValuePair<string, int> kvp in pending.IndexRoots)
            {
                string[] parts = kvp.Key.Split(':');
                if (parts.Length == 2)
                {
                    CollectionEntry collection = _collectionsMetadata.FindCollection(parts[0]);
                    if (collection != null)
                    {
                        IndexDefinition indexDef = collection.FindIndexByName(parts[1]);
                        if (indexDef != null)
                        {
                            indexDef.RootPageId = kvp.Value;
                        }
                    }
                }
            }

            pending.Clear();
        }
    }

    #endregion

    #region Internal Transaction Commit Methods

    internal DocumentLocation CommitInsert(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        DocumentLocation location = _documentStorage.WriteDocument(serializedData);

        BTree btree = GetPrimaryBTree(collection);
        btree.Insert(docId, location);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            SetPendingCollectionRoot(collection, currentRootPageId);
        }

        if (indexFields != null && indexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, location, indexFields);
        }

        _collectionsMetadata.UpdateCollection(collection);
        WriteCollectionsMetadataWithGrowth();

        return location;
    }

    internal DocumentLocation CommitUpdate(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> newIndexFields, IReadOnlyList<IndexFieldEntry> oldIndexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        BTree btree = GetPrimaryBTree(collection);

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        // Note: We do NOT delete the old document from storage for MVCC.
        // Old versions must remain readable until garbage collection.

        DocumentLocation newLocation = _documentStorage.WriteDocument(serializedData);

        btree.Update(docId, newLocation);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            SetPendingCollectionRoot(collection, currentRootPageId);
        }

        if (newIndexFields != null && newIndexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, newLocation, newIndexFields);
        }

        // Note: No need to call UpdateCollection - collection is a reference type
        // and modifications (like RootPage) are already reflected in the dictionary
        WriteCollectionsMetadataWithGrowth();

        return newLocation;
    }

    internal byte[] ReadDocumentByLocation(DocumentLocation location)
    {
        byte[] result;

        try
        {
            result = _documentStorage.ReadDocument(location.PageId, location.SlotIndex);
        }
        catch (DocumentSlotDeletedException)
        {
            // Document was GC'd after version was retrieved but before read.
            // This can happen due to the race window between GC committing physical
            // deletion and unlinking the version from VersionIndex. Return null to
            // signal that the document is no longer available.
            return null;
        }

        return result;
    }

    internal List<int> SearchDocIdRange(string collectionName, int startDocId, int endDocId, bool includeStart, bool includeEnd)
    {
        List<int> result = new List<int>();

        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection != null)
        {
            BTree btree = GetPrimaryBTree(collection);
            List<BTreeEntry> entries = btree.SearchRange(startDocId, endDocId, includeStart, includeEnd);

            foreach (BTreeEntry entry in entries)
            {
                result.Add(entry.Key);
            }
        }

        return result;
    }

    internal async Task<List<int>> SearchDocIdRangeAsync(string collectionName, int startDocId, int endDocId, bool includeStart, bool includeEnd, CancellationToken cancellationToken = default)
    {
        List<int> result = new List<int>();

        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection != null)
        {
            BTree btree = GetPrimaryBTree(collection);
            List<BTreeEntry> entries = await btree.SearchRangeAsync(startDocId, endDocId, includeStart, includeEnd, cancellationToken).ConfigureAwait(false);

            foreach (BTreeEntry entry in entries)
            {
                result.Add(entry.Key);
            }
        }

        return result;
    }

    internal List<SecondaryIndexEntry> SearchSecondaryIndex(string collectionName, IndexDefinition indexDef, byte[] keyBytes)
    {
        SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexDef);
        return indexTree.SearchByFieldValueWithDocIds(keyBytes);
    }

    internal List<SecondaryIndexEntry> SearchSecondaryIndexExact(string collectionName, IndexDefinition indexDef, byte[] keyBytes)
    {
        SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexDef);
        return indexTree.SearchByExactFieldValueWithDocIds(keyBytes);
    }

    internal List<SecondaryIndexEntry> SearchSecondaryIndexRange(string collectionName, IndexDefinition indexDef, byte[] startKeyBytes, byte[] endKeyBytes, bool includeStart, bool includeEnd)
    {
        SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexDef);
        return indexTree.SearchRangeWithDocIds(startKeyBytes, endKeyBytes, includeStart, includeEnd);
    }

    internal List<SecondaryIndexEntry> SearchSecondaryIndexPrefixRange(string collectionName, IndexDefinition indexDef, byte[] startKeyBytes, byte[] prefixKeyBytes, bool includeStart)
    {
        SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collectionName, indexDef);
        return indexTree.SearchPrefixRangeWithDocIds(startKeyBytes, prefixKeyBytes, includeStart);
    }

    internal void CommitDelete(string collectionName, int docId, IReadOnlyList<IndexFieldEntry> oldIndexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        BTree btree = GetPrimaryBTree(collection);

        // Note: We do NOT delete the document from storage for MVCC.
        // Old versions must remain readable until garbage collection.
        bool deleted = btree.Delete(docId);

        if (deleted)
        {
            int currentRootPageId = btree.GetRootPageId();
            if (currentRootPageId != collection.RootPage)
            {
                SetPendingCollectionRoot(collection, currentRootPageId);
            }

            WriteCollectionsMetadataWithGrowth();
        }
    }

    /// <summary>
    /// Applies document count changes after successful version validation.
    /// Thread-safe and persists the changes atomically.
    /// </summary>
    internal void ApplyDocumentCountDeltas(Dictionary<string, int> deltas)
    {
        lock (_documentCountLock)
        {
            if (deltas != null && deltas.Count > 0)
            {
                foreach (KeyValuePair<string, int> kvp in deltas)
                {
                    CollectionEntry collection = _collectionsMetadata.FindCollection(kvp.Key);
                    if (collection != null)
                    {
                        if (kvp.Value > 0)
                        {
                            for (int i = 0; i < kvp.Value; i++)
                            {
                                collection.IncrementDocumentCount();
                            }
                        }
                        else if (kvp.Value < 0)
                        {
                            for (int i = 0; i < -kvp.Value; i++)
                            {
                                collection.DecrementDocumentCount();
                            }
                        }

                        _collectionsMetadata.UpdateCollection(collection);
                    }
                }

                WriteCollectionsMetadataWithGrowth();
            }
        }
    }

    private void InsertIntoIndexesInternal(CollectionEntry collection, int docId, DocumentLocation location, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        // First pass: check unique constraints
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = FindIndexDefinition(collection, field.FieldName);
            if (indexDef != null && indexDef.IsUnique)
            {
                CheckUniqueConstraint(collection.Name, indexDef, field.FieldName, field.KeyBytes);
            }
        }

        // Second pass: insert into all indexes
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = FindIndexDefinition(collection, field.FieldName);
            if (indexDef != null)
            {
                SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collection.Name, indexDef);

                byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(field.KeyBytes, docId);
                indexTree.Insert(compositeKey, location);

                int currentRootPageId = indexTree.GetRootPageId();
                if (currentRootPageId != indexDef.RootPageId)
                {
                    SetPendingIndexRoot(collection, indexDef, currentRootPageId);
                }
            }
        }
    }

    private static IndexDefinition FindIndexDefinition(CollectionEntry collection, string indexNameOrFieldName)
    {
        IndexDefinition result = collection.FindIndex(indexNameOrFieldName);
        if (result == null)
        {
            result = collection.FindIndexByName(indexNameOrFieldName);
        }
        return result;
    }

    private void DeleteFromIndexesInternal(CollectionEntry collection, int docId, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = FindIndexDefinition(collection, field.FieldName);
            if (indexDef != null)
            {
                SecondaryIndexBTree indexTree = GetSecondaryIndexBTree(collection.Name, indexDef);

                byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(field.KeyBytes, docId);
                indexTree.Delete(compositeKey);

                int currentRootPageId = indexTree.GetRootPageId();
                if (currentRootPageId != indexDef.RootPageId)
                {
                    SetPendingIndexRoot(collection, indexDef, currentRootPageId);
                }
            }
        }
    }

    #endregion

    #region Internal Async Transaction Commit Methods

    internal async Task<DocumentLocation> CommitInsertAsync(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> indexFields, CancellationToken cancellationToken = default)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        DocumentLocation location = await _documentStorage.WriteDocumentAsync(serializedData, cancellationToken).ConfigureAwait(false);

        BTree btree = GetPrimaryBTree(collection);
        await btree.InsertAsync(docId, location, cancellationToken).ConfigureAwait(false);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            SetPendingCollectionRoot(collection, currentRootPageId);
        }

        if (indexFields != null && indexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, location, indexFields);
        }

        _collectionsMetadata.UpdateCollection(collection);
        WriteCollectionsMetadataWithGrowth();

        return location;
    }

    internal async Task<DocumentLocation> CommitUpdateAsync(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> newIndexFields, IReadOnlyList<IndexFieldEntry> oldIndexFields, CancellationToken cancellationToken = default)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        BTree btree = GetPrimaryBTree(collection);

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        // Note: We do NOT delete the old document from storage for MVCC.
        // Old versions must remain readable until garbage collection.

        DocumentLocation newLocation = await _documentStorage.WriteDocumentAsync(serializedData, cancellationToken).ConfigureAwait(false);

        await btree.DeleteAsync(docId, cancellationToken).ConfigureAwait(false);
        await btree.InsertAsync(docId, newLocation, cancellationToken).ConfigureAwait(false);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            SetPendingCollectionRoot(collection, currentRootPageId);
        }

        if (newIndexFields != null && newIndexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, newLocation, newIndexFields);
        }

        _collectionsMetadata.UpdateCollection(collection);
        WriteCollectionsMetadataWithGrowth();

        return newLocation;
    }

    internal async Task<byte[]> ReadDocumentByLocationAsync(DocumentLocation location, CancellationToken cancellationToken = default)
    {
        byte[] result;

        try
        {
            result = await _documentStorage.ReadDocumentAsync(location.PageId, location.SlotIndex, cancellationToken).ConfigureAwait(false);
        }
        catch (DocumentSlotDeletedException)
        {
            // Document was GC'd after version was retrieved but before read.
            // This can happen due to the race window between GC committing physical
            // deletion and unlinking the version from VersionIndex. Return null to
            // signal that the document is no longer available.
            return null;
        }

        return result;
    }

    internal async Task CommitDeleteAsync(string collectionName, int docId, IReadOnlyList<IndexFieldEntry> oldIndexFields, CancellationToken cancellationToken = default)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        BTree btree = GetPrimaryBTree(collection);

        // Note: We do NOT delete the document from storage for MVCC.
        // Old versions must remain readable until garbage collection.
        bool deleted = await btree.DeleteAsync(docId, cancellationToken).ConfigureAwait(false);

        if (deleted)
        {
            int currentRootPageId = btree.GetRootPageId();
            if (currentRootPageId != collection.RootPage)
            {
                SetPendingCollectionRoot(collection, currentRootPageId);
            }

            WriteCollectionsMetadataWithGrowth();
        }
    }

    #endregion

    #region Compaction Methods

    private void InsertRawForCompaction(GaldrDb targetDb, string collectionName, int docId, byte[] docBytes, IGaldrTypeInfo typeInfo)
    {
        CollectionEntry collection = targetDb._collectionsMetadata.FindCollection(collectionName);

        DocumentLocation location = targetDb._documentStorage.WriteDocument(docBytes);

        BTree btree = targetDb.GetPrimaryBTree(collection);
        btree.Insert(docId, location);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            collection.RootPage = currentRootPageId;
        }

        if (collection.Indexes.Count > 0)
        {
            string json = Encoding.UTF8.GetString(docBytes);
            if (_jsonSerializer.TryDeserialize(json, typeInfo.DocumentType, out object document, _jsonOptions))
            {
                IndexFieldWriter writer = new IndexFieldWriter();
                typeInfo.ExtractIndexedFieldsFrom(document, writer);
                IReadOnlyList<IndexFieldEntry> indexFields = writer.GetFields();

                InsertIntoIndexesForCompaction(targetDb, collection, docId, location, indexFields);
            }
        }

        collection.IncrementDocumentCount();
        targetDb._collectionsMetadata.UpdateCollection(collection);
        targetDb.WriteCollectionsMetadataWithGrowth();
    }

    private async Task InsertRawForCompactionAsync(GaldrDb targetDb, string collectionName, int docId, byte[] docBytes, IGaldrTypeInfo typeInfo, CancellationToken cancellationToken)
    {
        CollectionEntry collection = targetDb._collectionsMetadata.FindCollection(collectionName);

        DocumentLocation location = await targetDb._documentStorage.WriteDocumentAsync(docBytes, cancellationToken).ConfigureAwait(false);

        BTree btree = targetDb.GetPrimaryBTree(collection);
        await btree.InsertAsync(docId, location, cancellationToken).ConfigureAwait(false);

        int currentRootPageId = btree.GetRootPageId();
        if (currentRootPageId != collection.RootPage)
        {
            collection.RootPage = currentRootPageId;
        }

        if (collection.Indexes.Count > 0)
        {
            string json = Encoding.UTF8.GetString(docBytes);
            if (_jsonSerializer.TryDeserialize(json, typeInfo.DocumentType, out object document, _jsonOptions))
            {
                IndexFieldWriter writer = new IndexFieldWriter();
                typeInfo.ExtractIndexedFieldsFrom(document, writer);
                IReadOnlyList<IndexFieldEntry> indexFields = writer.GetFields();

                InsertIntoIndexesForCompaction(targetDb, collection, docId, location, indexFields);
            }
        }

        collection.IncrementDocumentCount();
        targetDb._collectionsMetadata.UpdateCollection(collection);
        targetDb.WriteCollectionsMetadataWithGrowth();
    }

    private void InsertIntoIndexesForCompaction(GaldrDb targetDb, CollectionEntry collection, int docId, DocumentLocation location, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = FindIndexDefinition(collection, field.FieldName);
            if (indexDef != null)
            {
                if (indexDef.RootPageId == -1)
                {
                    int rootPageId = targetDb._pageManager.AllocatePage();
                    int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(targetDb._options.UsablePageSize);
                    SecondaryIndexNode rootNode = new SecondaryIndexNode(targetDb._options.UsablePageSize, maxKeys, BTreeNodeType.Leaf);

                    byte[] rootBuffer = BufferPool.Rent(targetDb._options.PageSize);
                    try
                    {
                        rootNode.SerializeTo(rootBuffer);
                        targetDb._pageIO.WritePage(rootPageId, rootBuffer);
                    }
                    finally
                    {
                        BufferPool.Return(rootBuffer);
                    }

                    indexDef.RootPageId = rootPageId;
                }

                SecondaryIndexBTree indexTree = targetDb.GetSecondaryIndexBTree(collection.Name, indexDef);

                byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(field.KeyBytes, docId);
                indexTree.Insert(compositeKey, location);

                int newRootPageId = indexTree.GetRootPageId();
                if (newRootPageId != indexDef.RootPageId)
                {
                    indexDef.RootPageId = newRootPageId;
                }
            }
        }
    }

    #endregion
}
