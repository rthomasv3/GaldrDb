using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.WAL;
using GaldrJson;

namespace GaldrDbEngine;

public class GaldrDb : IDisposable
{
    #region Fields

    private readonly string _filePath;
    private readonly GaldrDbOptions _options;
    private readonly string _walPath;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;
    private readonly HashSet<string> _ensuredCollections;
    private readonly object _ddlLock;
    private IPageIO _basePageIO;
    private IPageIO _pageIO;
    private WriteAheadLog _wal;
    private WalPageIO _walPageIO;
    private PageManager _pageManager;
    private DocumentStorage _documentStorage;
    private CollectionsMetadata _collectionsMetadata;
    private TransactionManager _txManager;
    private VersionIndex _versionIndex;
    private VersionGarbageCollector _garbageCollector;
    private long _lastGCCommitCount;

    #endregion

    #region Constructor

    public GaldrDb(string filePath, GaldrDbOptions options)
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
    }

    #endregion

    #region Public Methods

    public static GaldrDb Create(string filePath, GaldrDbOptions options)
    {
        GaldrDb db = new GaldrDb(filePath, options);
        db.ValidateOptions();
        db.InitializeFile();
        db.InitializePools();
        return db;
    }

    public static GaldrDb Open(string filePath, GaldrDbOptions options = null)
    {
        if (options == null || options.PageSize == 0)
        {
            byte[] peekBuffer = new byte[12];
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fs.ReadExactly(peekBuffer, 0, 12);
            }
            int filePageSize = BinaryHelper.ReadInt32LE(peekBuffer, 8);

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
        db.OpenAndValidateFile();
        db.InitializePools();

        return db;
    }

    public void Dispose()
    {
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

        if (_basePageIO != null)
        {
            _basePageIO.Close();
            _basePageIO.Dispose();
            _basePageIO = null;
        }

        _pageIO = null;
        _walPageIO = null;
    }

    public void Checkpoint()
    {
        if (_walPageIO != null)
        {
            _walPageIO.Checkpoint();
        }
    }

    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        if (_walPageIO != null)
        {
            await _walPageIO.CheckpointAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public GarbageCollectionResult Vacuum()
    {
        EnsureGarbageCollector();
        GarbageCollectionResult gcResult = _garbageCollector.Collect();

        if (gcResult.VersionsCollected > 0)
        {
            ulong walTxId = _txManager.AllocateTxId().Value;
            BeginWalTransaction(walTxId);

            try
            {
                foreach (CollectableVersion collectable in gcResult.CollectableVersions)
                {
                    _documentStorage.DeleteDocument(collectable.Location.PageId, collectable.Location.SlotIndex);
                }

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }

            _garbageCollector.UnlinkVersions(gcResult.CollectableVersions);
        }

        int pagesCompacted = CompactFragmentedPages();

        return new GarbageCollectionResult(
            gcResult.VersionsCollected,
            gcResult.DocumentsProcessed,
            gcResult.CollectableVersions,
            pagesCompacted);
    }

    public Task<GarbageCollectionResult> VacuumAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(Vacuum());
    }

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

        using (GaldrDb targetDb = GaldrDb.Create(targetPath, _options))
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
                    IndexDefinition newIndex = new IndexDefinition
                    {
                        FieldName = sourceIndex.FieldName,
                        FieldType = sourceIndex.FieldType,
                        RootPageId = -1,
                        IsUnique = sourceIndex.IsUnique
                    };
                    targetCollection.Indexes.Add(newIndex);
                }

                targetDb._collectionsMetadata.UpdateCollection(targetCollection);
                targetDb._collectionsMetadata.WriteToDisk();

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

        using (GaldrDb targetDb = GaldrDb.Create(targetPath, _options))
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
                    IndexDefinition newIndex = new IndexDefinition
                    {
                        FieldName = sourceIndex.FieldName,
                        FieldType = sourceIndex.FieldType,
                        RootPageId = -1,
                        IsUnique = sourceIndex.IsUnique
                    };
                    targetCollection.Indexes.Add(newIndex);
                }

                targetDb._collectionsMetadata.UpdateCollection(targetCollection);
                targetDb._collectionsMetadata.WriteToDisk();

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

    public Transaction BeginTransaction()
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        TxId txId = _txManager.AllocateTxId();
        TxId snapshotTxId = _txManager.GetSnapshotTxId();

        Transaction tx = new Transaction(
            this,
            _txManager,
            _versionIndex,
            txId,
            snapshotTxId,
            false,
            _jsonSerializer,
            _jsonOptions);

        _txManager.RegisterTransaction(txId, snapshotTxId);

        return tx;
    }

    public Transaction BeginReadOnlyTransaction()
    {
        EnsureTransactionManager();
        EnsureVersionIndex();

        TxId txId = _txManager.AllocateTxId();
        TxId snapshotTxId = _txManager.GetSnapshotTxId();

        Transaction tx = new Transaction(
            this,
            _txManager,
            _versionIndex,
            txId,
            snapshotTxId,
            true,
            _jsonSerializer,
            _jsonOptions);

        _txManager.RegisterTransaction(txId, snapshotTxId);

        return tx;
    }

    private void CreateCollection(string collectionName)
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

            int order = CalculateBTreeOrder(_options.PageSize);
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
            _collectionsMetadata.WriteToDisk();

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
                int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, indexToRemove.RootPageId, _options.PageSize, maxKeys);

                List<int> pageIds = indexTree.CollectAllPageIds();
                for (int i = 0; i < pageIds.Count; i++)
                {
                    _pageManager.DeallocatePage(pageIds[i]);
                }

                collection.Indexes.RemoveAt(indexPosition);
                _collectionsMetadata.UpdateCollection(collection);
                _collectionsMetadata.WriteToDisk();

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
        }
    }

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
                    int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                    SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, index.RootPageId, _options.PageSize, maxKeys);

                    List<int> indexPageIds = indexTree.CollectAllPageIds();
                    for (int j = 0; j < indexPageIds.Count; j++)
                    {
                        _pageManager.DeallocatePage(indexPageIds[j]);
                    }
                }

                int order = CalculateBTreeOrder(_options.PageSize);
                BTree primaryTree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);

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
                _collectionsMetadata.WriteToDisk();

                _ensuredCollections.Remove(collectionName);

                CommitWalTransaction();
            }
            catch
            {
                AbortWalTransaction();
                throw;
            }
        }
    }

    #endregion

    #region Type-Safe CRUD Operations

    public int Insert<T>(T document)
    {
        using (Transaction tx = BeginTransaction())
        {
            int id = tx.Insert<T>(document);
            tx.Commit();
            return id;
        }
    }

    public T GetById<T>(int id)
    {
        using (Transaction tx = BeginReadOnlyTransaction())
        {
            T result = tx.GetById<T>(id);
            tx.Commit();
            return result;
        }
    }

    public bool Update<T>(T document)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = tx.Update<T>(document);
            tx.Commit();
            return result;
        }
    }

    public bool Delete<T>(int id)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = tx.Delete<T>(id);
            tx.Commit();
            return result;
        }
    }

    public QueryBuilder<T> Query<T>()
    {
        Transaction tx = BeginReadOnlyTransaction();
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

    public async Task<int> InsertAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        using (Transaction tx = BeginTransaction())
        {
            int id = await tx.InsertAsync<T>(document, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return id;
        }
    }

    public async Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        using (Transaction tx = BeginReadOnlyTransaction())
        {
            T result = await tx.GetByIdAsync<T>(id, cancellationToken).ConfigureAwait(false);
            tx.Commit();
            return result;
        }
    }

    public async Task<bool> UpdateAsync<T>(T document, CancellationToken cancellationToken = default)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = await tx.UpdateAsync<T>(document, cancellationToken).ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
    }

    public async Task<bool> DeleteAsync<T>(int id, CancellationToken cancellationToken = default)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = await tx.DeleteAsync<T>(id, cancellationToken).ConfigureAwait(false);
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
        List<IndexDefinition> newIndexes = new List<IndexDefinition>();

        foreach (string fieldName in indexedFieldNames)
        {
            IndexDefinition existingIndex = collection.FindIndex(fieldName);
            if (existingIndex == null)
            {
                bool isUnique = ContainsFieldName(uniqueIndexFieldNames, fieldName);

                IndexDefinition newIndex = new IndexDefinition
                {
                    FieldName = fieldName,
                    FieldType = GaldrFieldType.String,
                    RootPageId = -1,
                    IsUnique = isUnique
                };

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
                    int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                    SecondaryIndexNode rootNode = new SecondaryIndexNode(_options.PageSize, maxKeys, BTreeNodeType.Leaf);

                    rootNode.SerializeTo(rootBuffer);
                    _pageIO.WritePage(rootPageId, rootBuffer);

                    newIndex.RootPageId = rootPageId;
                    collection.Indexes.Add(newIndex);
                }

                _collectionsMetadata.UpdateCollection(collection);
                _collectionsMetadata.WriteToDisk();

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
    
    private void CheckUniqueConstraint(IndexDefinition indexDef, string fieldName, byte[] keyBytes)
    {
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
        SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, indexDef.RootPageId, _options.PageSize, maxKeys);

        List<DocumentLocation> existingEntries = indexTree.SearchByFieldValue(keyBytes);
        if (existingEntries.Count > 0)
        {
            throw new InvalidOperationException($"Unique constraint violation: A document with {fieldName} = '{GetFieldValueString(keyBytes)}' already exists.");
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

            int order = CalculateBTreeOrder(_options.PageSize);
            int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);

            BTreeNodePool.Warmup(_options.PageSize, order, 8);
            SecondaryIndexNodePool.Warmup(_options.PageSize, maxKeys, 8);
        }
    }

    private void InitializeFile()
    {
        if (File.Exists(_filePath))
        {
            throw new InvalidOperationException("File exists; use Open.");
        }

        SetupIO();

        // Initialize using base page IO first
        _pageManager = new PageManager(_basePageIO, _options.PageSize);
        _pageManager.Initialize();

        _collectionsMetadata = new CollectionsMetadata(_basePageIO, _pageManager.Header.CollectionsMetadataPage, _options.PageSize);
        _collectionsMetadata.WriteToDisk();

        _pageManager.Flush();

        if (_options.UseWal)
        {
            InitializeWalFile();
        }

        // Document storage uses the final _pageIO (WAL-wrapped if UseWal is true)
        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _options.PageSize);

        EnsureAllCollections();
    }

    private void OpenAndValidateFile()
    {
        if (!File.Exists(_filePath))
        {
            throw new FileNotFoundException();
        }

        SetupIO();

        _pageManager = new PageManager(_basePageIO, _options.PageSize);
        _pageManager.Load();

        if (_options.PageSize == 0 || _options.PageSize != _pageManager.Header.PageSize)
        {
            _options.PageSize = _pageManager.Header.PageSize;
        }

        _collectionsMetadata = new CollectionsMetadata(_basePageIO, _pageManager.Header.CollectionsMetadataPage, _pageManager.Header.PageSize);
        _collectionsMetadata.LoadFromDisk();

        _documentStorage = new DocumentStorage(_basePageIO, _pageManager, _pageManager.Header.PageSize);

        if (_options.UseWal)
        {
            if (File.Exists(_walPath))
            {
                RecoverFromWal();
            }
            else
            {
                InitializeWalFile();
                // Update document storage to use WAL page IO
                _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageManager.Header.PageSize);

                // Rebuild VersionIndex from current database state
                RebuildVersionIndex(0);
            }
        }
        else
        {
            // No WAL - still need to rebuild VersionIndex for MVCC reads to work
            RebuildVersionIndex(0);
        }

        EnsureAllCollections();
    }

    private void SetupIO()
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

        _pageIO = _basePageIO;
    }

    private void InitializeWalFile()
    {
        _wal = new WriteAheadLog(_walPath, _options.PageSize);
        _wal.Create();

        _walPageIO = new WalPageIO(_basePageIO, _wal, _options.PageSize);
        _pageIO = _walPageIO;
    }

    private void RecoverFromWal()
    {
        _wal = new WriteAheadLog(_walPath, _options.PageSize);
        _wal.Open();

        // Get all committed transactions
        HashSet<ulong> committedTxIds = _wal.GetCommittedTransactions();
        Dictionary<ulong, List<WalFrame>> framesByTx = _wal.GetFramesByTransaction();

        // Find the highest committed TxId for TransactionManager initialization
        ulong maxCommittedTxId = 0;

        // Apply committed transactions in order
        List<ulong> sortedTxIds = new List<ulong>(committedTxIds);
        sortedTxIds.Sort();

        foreach (ulong txId in sortedTxIds)
        {
            if (framesByTx.TryGetValue(txId, out List<WalFrame> frames))
            {
                foreach (WalFrame frame in frames)
                {
                    if (frame.PageId >= 0 && frame.Data.Length > 0)
                    {
                        _basePageIO.WritePage(frame.PageId, frame.Data);
                    }
                }
            }

            if (txId > maxCommittedTxId)
            {
                maxCommittedTxId = txId;
            }
        }

        _basePageIO.Flush();

        // Reload page manager and metadata after recovery
        _pageManager = new PageManager(_basePageIO, _options.PageSize);
        _pageManager.Load();

        _collectionsMetadata = new CollectionsMetadata(_basePageIO, _pageManager.Header.CollectionsMetadataPage, _pageManager.Header.PageSize);
        _collectionsMetadata.LoadFromDisk();

        // Truncate WAL after successful recovery
        _wal.Truncate();

        // Set up WAL page IO for future operations
        _walPageIO = new WalPageIO(_basePageIO, _wal, _options.PageSize);
        _pageIO = _walPageIO;

        // Update the document storage to use the new page IO
        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageManager.Header.PageSize);

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
        int order = CalculateBTreeOrder(_options.PageSize);

        foreach (CollectionEntry collection in collections)
        {
            _versionIndex.EnsureCollection(collection.Name);

            BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
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

        int result = order;

        return result;
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
        if (!_options.AutoGarbageCollection)
        {
            return;
        }

        long currentCommitCount = _txManager.CommitCount;
        long commitsSinceLastGC = currentCommitCount - _lastGCCommitCount;

        if (commitsSinceLastGC >= _options.GarbageCollectionThreshold)
        {
            Vacuum();
            _lastGCCommitCount = currentCommitCount;
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

    internal void BeginWalTransaction(ulong txId)
    {
        if (_walPageIO != null)
        {
            _walPageIO.BeginTransaction(txId);
        }
    }

    internal void CommitWalTransaction()
    {
        if (_walPageIO != null)
        {
            _walPageIO.CommitTransaction();
        }
    }

    internal void AbortWalTransaction()
    {
        if (_walPageIO != null)
        {
            _walPageIO.AbortTransaction();
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

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        btree.Insert(docId, location);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        if (indexFields != null && indexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, location, indexFields);
        }

        collection.DocumentCount++;
        if (docId >= collection.NextId)
        {
            collection.NextId = docId + 1;
        }
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return location;
    }

    internal DocumentLocation CommitUpdate(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> newIndexFields, IReadOnlyList<IndexFieldEntry> oldIndexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        // Note: We do NOT delete the old document from storage for MVCC.
        // Old versions must remain readable until garbage collection.

        DocumentLocation newLocation = _documentStorage.WriteDocument(serializedData);

        btree.Delete(docId);
        btree.Insert(docId, newLocation);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        if (newIndexFields != null && newIndexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, newLocation, newIndexFields);
        }

        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return newLocation;
    }

    internal byte[] ReadDocumentByLocation(DocumentLocation location)
    {
        return _documentStorage.ReadDocument(location.PageId, location.SlotIndex);
    }

    internal List<int> SearchDocIdRange(string collectionName, int startDocId, int endDocId, bool includeStart, bool includeEnd)
    {
        List<int> result = new List<int>();

        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection != null)
        {
            int order = CalculateBTreeOrder(_options.PageSize);
            BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
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
            int order = CalculateBTreeOrder(_options.PageSize);
            BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
            List<BTreeEntry> entries = await btree.SearchRangeAsync(startDocId, endDocId, includeStart, includeEnd, cancellationToken).ConfigureAwait(false);

            foreach (BTreeEntry entry in entries)
            {
                result.Add(entry.Key);
            }
        }

        return result;
    }

    internal void CommitDelete(string collectionName, int docId, IReadOnlyList<IndexFieldEntry> oldIndexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation? location = btree.Search(docId);

        if (location != null)
        {
            if (oldIndexFields != null && oldIndexFields.Count > 0)
            {
                DeleteFromIndexesInternal(collection, docId, oldIndexFields);
            }

            // Note: We do NOT delete the document from storage for MVCC.
            // Old versions must remain readable until garbage collection.
            btree.Delete(docId);

            int newRootPageId = btree.GetRootPageId();
            if (newRootPageId != collection.RootPage)
            {
                collection.RootPage = newRootPageId;
            }

            collection.DocumentCount--;
            _collectionsMetadata.UpdateCollection(collection);
            _collectionsMetadata.WriteToDisk();
        }
    }

    private void InsertIntoIndexesInternal(CollectionEntry collection, int docId, DocumentLocation location, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        // First pass: check unique constraints
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = collection.FindIndex(field.FieldName);
            if (indexDef != null && indexDef.IsUnique)
            {
                CheckUniqueConstraint(indexDef, field.FieldName, field.KeyBytes);
            }
        }

        // Second pass: insert into all indexes
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = collection.FindIndex(field.FieldName);
            if (indexDef != null)
            {
                int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, indexDef.RootPageId, _options.PageSize, maxKeys);

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

    private void DeleteFromIndexesInternal(CollectionEntry collection, int docId, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = collection.FindIndex(field.FieldName);
            if (indexDef != null)
            {
                int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, indexDef.RootPageId, _options.PageSize, maxKeys);

                byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(field.KeyBytes, docId);
                indexTree.Delete(compositeKey);
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

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        await btree.InsertAsync(docId, location, cancellationToken).ConfigureAwait(false);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        if (indexFields != null && indexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, location, indexFields);
        }

        collection.DocumentCount++;
        if (docId >= collection.NextId)
        {
            collection.NextId = docId + 1;
        }
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return location;
    }

    internal async Task<DocumentLocation> CommitUpdateAsync(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> newIndexFields, IReadOnlyList<IndexFieldEntry> oldIndexFields, CancellationToken cancellationToken = default)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        // Note: We do NOT delete the old document from storage for MVCC.
        // Old versions must remain readable until garbage collection.

        DocumentLocation newLocation = await _documentStorage.WriteDocumentAsync(serializedData, cancellationToken).ConfigureAwait(false);

        await btree.DeleteAsync(docId, cancellationToken).ConfigureAwait(false);
        await btree.InsertAsync(docId, newLocation, cancellationToken).ConfigureAwait(false);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        if (newIndexFields != null && newIndexFields.Count > 0)
        {
            InsertIntoIndexesInternal(collection, docId, newLocation, newIndexFields);
        }

        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return newLocation;
    }

    internal async Task<byte[]> ReadDocumentByLocationAsync(DocumentLocation location, CancellationToken cancellationToken = default)
    {
        byte[] result = await _documentStorage.ReadDocumentAsync(location.PageId, location.SlotIndex, cancellationToken).ConfigureAwait(false);
        return result;
    }

    internal async Task CommitDeleteAsync(string collectionName, int docId, IReadOnlyList<IndexFieldEntry> oldIndexFields, CancellationToken cancellationToken = default)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation? location = await btree.SearchAsync(docId, cancellationToken).ConfigureAwait(false);

        if (location != null)
        {
            if (oldIndexFields != null && oldIndexFields.Count > 0)
            {
                DeleteFromIndexesInternal(collection, docId, oldIndexFields);
            }

            // Note: We do NOT delete the document from storage for MVCC.
            // Old versions must remain readable until garbage collection.
            await btree.DeleteAsync(docId, cancellationToken).ConfigureAwait(false);

            int newRootPageId = btree.GetRootPageId();
            if (newRootPageId != collection.RootPage)
            {
                collection.RootPage = newRootPageId;
            }

            collection.DocumentCount--;
            _collectionsMetadata.UpdateCollection(collection);
            _collectionsMetadata.WriteToDisk();
        }
    }

    #endregion

    #region Compaction Methods

    private void InsertRawForCompaction(GaldrDb targetDb, string collectionName, int docId, byte[] docBytes, IGaldrTypeInfo typeInfo)
    {
        CollectionEntry collection = targetDb._collectionsMetadata.FindCollection(collectionName);

        DocumentLocation location = targetDb._documentStorage.WriteDocument(docBytes);

        int order = CalculateBTreeOrder(targetDb._options.PageSize);
        BTree btree = new BTree(targetDb._pageIO, targetDb._pageManager, collection.RootPage, targetDb._options.PageSize, order);
        btree.Insert(docId, location);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
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

        collection.DocumentCount++;
        targetDb._collectionsMetadata.UpdateCollection(collection);
        targetDb._collectionsMetadata.WriteToDisk();
    }

    private async Task InsertRawForCompactionAsync(GaldrDb targetDb, string collectionName, int docId, byte[] docBytes, IGaldrTypeInfo typeInfo, CancellationToken cancellationToken)
    {
        CollectionEntry collection = targetDb._collectionsMetadata.FindCollection(collectionName);

        DocumentLocation location = await targetDb._documentStorage.WriteDocumentAsync(docBytes, cancellationToken).ConfigureAwait(false);

        int order = CalculateBTreeOrder(targetDb._options.PageSize);
        BTree btree = new BTree(targetDb._pageIO, targetDb._pageManager, collection.RootPage, targetDb._options.PageSize, order);
        await btree.InsertAsync(docId, location, cancellationToken).ConfigureAwait(false);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
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

        collection.DocumentCount++;
        targetDb._collectionsMetadata.UpdateCollection(collection);
        targetDb._collectionsMetadata.WriteToDisk();
    }

    private void InsertIntoIndexesForCompaction(GaldrDb targetDb, CollectionEntry collection, int docId, DocumentLocation location, IReadOnlyList<IndexFieldEntry> indexFields)
    {
        foreach (IndexFieldEntry field in indexFields)
        {
            IndexDefinition indexDef = collection.FindIndex(field.FieldName);
            if (indexDef != null)
            {
                if (indexDef.RootPageId == -1)
                {
                    int rootPageId = targetDb._pageManager.AllocatePage();
                    int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(targetDb._options.PageSize);
                    SecondaryIndexNode rootNode = new SecondaryIndexNode(targetDb._options.PageSize, maxKeys, BTreeNodeType.Leaf);

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

                int maxKeysForTree = SecondaryIndexBTree.CalculateMaxKeys(targetDb._options.PageSize);
                SecondaryIndexBTree indexTree = new SecondaryIndexBTree(targetDb._pageIO, targetDb._pageManager, indexDef.RootPageId, targetDb._options.PageSize, maxKeysForTree);

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
