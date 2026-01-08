using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using GaldrDbEngine.IO;
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
    private IPageIO _basePageIO;
    private IPageIO _pageIO;
    private WriteAheadLog _wal;
    private WalPageIO _walPageIO;
    private PageManager _pageManager;
    private DocumentStorage _documentStorage;
    private CollectionsMetadata _collectionsMetadata;
    private TransactionManager _txManager;

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
    }

    #endregion

    #region Public Methods

    public static GaldrDb Create(string filePath, GaldrDbOptions options)
    {
        GaldrDb db = new GaldrDb(filePath, options);
        db.ValidateOptions();
        db.InitializeFile();
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

    public Transaction BeginTransaction()
    {
        EnsureTransactionManager();

        TxId txId = _txManager.AllocateTxId();
        TxId snapshotTxId = _txManager.GetSnapshotTxId();

        Transaction tx = new Transaction(
            this,
            _txManager,
            txId,
            snapshotTxId,
            isReadOnly: false,
            _jsonSerializer,
            _jsonOptions);

        _txManager.RegisterTransaction(txId);

        return tx;
    }

    public Transaction BeginReadOnlyTransaction()
    {
        EnsureTransactionManager();

        TxId txId = _txManager.AllocateTxId();
        TxId snapshotTxId = _txManager.GetSnapshotTxId();

        Transaction tx = new Transaction(
            this,
            _txManager,
            txId,
            snapshotTxId,
            isReadOnly: true,
            _jsonSerializer,
            _jsonOptions);

        _txManager.RegisterTransaction(txId);

        return tx;
    }

    public void CreateCollection(string collectionName)
    {
        CollectionEntry existingCollection = _collectionsMetadata.FindCollection(collectionName);
        if (existingCollection != null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' already exists");
        }

        int rootPageId = _pageManager.AllocatePage();

        int order = CalculateBTreeOrder(_options.PageSize);
        BTreeNode rootNode = new BTreeNode(_options.PageSize, order, BTreeNodeType.Leaf);
        byte[] rootBytes = rootNode.Serialize();
        _pageIO.WritePage(rootPageId, rootBytes);

        _collectionsMetadata.AddCollection(collectionName, rootPageId);
        _collectionsMetadata.WriteToDisk();

        _pageManager.SetFreeSpaceLevel(rootPageId, FreeSpaceLevel.None);
    }

    public int InsertDocument<T>(string collectionName, T document)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
        DocumentLocation location = _documentStorage.WriteDocument(jsonBytes);

        int docId = collection.NextId;
        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        btree.Insert(docId, location);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        collection.DocumentCount++;
        collection.NextId++;
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return docId;
    }

    public T GetDocument<T>(string collectionName, int docId)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation location = btree.Search(docId);

        if (location == null)
        {
            return default(T);
        }

        byte[] jsonBytes = _documentStorage.ReadDocument(location.PageId, location.SlotIndex);
        string json = Encoding.UTF8.GetString(jsonBytes);
        T result = _jsonSerializer.Deserialize<T>(json, _jsonOptions);

        return result;
    }

    public bool DeleteDocument(string collectionName, int docId)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation location = btree.Search(docId);

        if (location == null)
        {
            return false;
        }

        _documentStorage.DeleteDocument(location.PageId, location.SlotIndex);

        btree.Delete(docId);

        collection.DocumentCount--;
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return true;
    }

    public bool UpdateDocument<T>(string collectionName, int docId, T document)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation oldLocation = btree.Search(docId);

        if (oldLocation == null)
        {
            return false;
        }

        _documentStorage.DeleteDocument(oldLocation.PageId, oldLocation.SlotIndex);

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
        DocumentLocation newLocation = _documentStorage.WriteDocument(jsonBytes);

        btree.Delete(docId);
        btree.Insert(docId, newLocation);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
            _collectionsMetadata.UpdateCollection(collection);
            _collectionsMetadata.WriteToDisk();
        }

        return true;
    }

    #endregion

    #region Type-Safe CRUD Operations

    public void EnsureCollection<T>()
    {
        EnsureCollection(GaldrTypeRegistry.Get<T>());
    }

    public int Insert<T>(T document)
    {
        return Insert(document, GaldrTypeRegistry.Get<T>());
    }

    public T GetById<T>(int id)
    {
        return GetById(id, GaldrTypeRegistry.Get<T>());
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

    public List<T> GetAllDocuments<T>()
    {
        return GetAllDocuments(GaldrTypeRegistry.Get<T>());
    }

    public void EnsureCollection<T>(GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;

        if (!_ensuredCollections.Contains(collectionName))
        {
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

    public int Insert<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        using (Transaction tx = BeginTransaction())
        {
            int id = tx.Insert(document, typeInfo);
            tx.Commit();
            return id;
        }
    }

    public T GetById<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;
        T result = GetDocument<T>(collectionName, id);
        return result;
    }

    public bool Update<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = tx.Update(document, typeInfo);
            tx.Commit();
            return result;
        }
    }

    public bool Delete<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        using (Transaction tx = BeginTransaction())
        {
            bool result = tx.Delete<T>(id, typeInfo);
            tx.Commit();
            return result;
        }
    }

    public QueryBuilder<T> Query<T>(GaldrTypeInfo<T> typeInfo)
    {
        DatabaseQueryExecutor<T> executor = new DatabaseQueryExecutor<T>(this, typeInfo);
        QueryBuilder<T> queryBuilder = new QueryBuilder<T>(executor);
        return queryBuilder;
    }

    public List<T> GetAllDocuments<T>(GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);

        if (collection == null)
        {
            return new List<T>();
        }

        List<T> results = new List<T>();
        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);

        List<BTreeEntry> allEntries = btree.GetAllEntries();

        foreach (BTreeEntry entry in allEntries)
        {
            byte[] jsonBytes = _documentStorage.ReadDocument(entry.Location.PageId, entry.Location.SlotIndex);
            string json = Encoding.UTF8.GetString(jsonBytes);
            T document = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            results.Add(document);
        }

        return results;
    }

    public CollectionEntry GetCollection(string collectionName)
    {
        return _collectionsMetadata.FindCollection(collectionName);
    }

    public List<T> GetDocumentsByLocations<T>(List<DocumentLocation> locations, GaldrTypeInfo<T> typeInfo)
    {
        List<T> results = new List<T>();

        foreach (DocumentLocation location in locations)
        {
            byte[] jsonBytes = _documentStorage.ReadDocument(location.PageId, location.SlotIndex);
            string json = Encoding.UTF8.GetString(jsonBytes);
            T document = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            results.Add(document);
        }

        return results;
    }

    public SecondaryIndexBTree GetSecondaryIndexTree(IndexDefinition indexDef)
    {
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
        SecondaryIndexBTree indexTree = new SecondaryIndexBTree(_pageIO, _pageManager, indexDef.RootPageId, _options.PageSize, maxKeys);
        return indexTree;
    }

    #endregion

    #region Index Operations

    private void EnsureIndexes<T>(CollectionEntry collection, GaldrTypeInfo<T> typeInfo)
    {
        IReadOnlyList<string> indexedFieldNames = typeInfo.IndexedFieldNames;
        IReadOnlyList<string> uniqueIndexFieldNames = typeInfo.UniqueIndexFieldNames;
        bool metadataChanged = false;

        foreach (string fieldName in indexedFieldNames)
        {
            IndexDefinition existingIndex = collection.FindIndex(fieldName);
            if (existingIndex == null)
            {
                int rootPageId = _pageManager.AllocatePage();
                int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(_options.PageSize);
                SecondaryIndexNode rootNode = new SecondaryIndexNode(_options.PageSize, maxKeys, BTreeNodeType.Leaf);
                byte[] rootBytes = rootNode.Serialize();
                _pageIO.WritePage(rootPageId, rootBytes);

                bool isUnique = ContainsFieldName(uniqueIndexFieldNames, fieldName);

                IndexDefinition newIndex = new IndexDefinition
                {
                    FieldName = fieldName,
                    FieldType = GaldrFieldType.String,
                    RootPageId = rootPageId,
                    IsUnique = isUnique
                };

                collection.Indexes.Add(newIndex);
                metadataChanged = true;
            }
        }

        if (metadataChanged)
        {
            _collectionsMetadata.UpdateCollection(collection);
            _collectionsMetadata.WriteToDisk();
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
            }
        }
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

    #endregion

    #region Internal Transaction Commit Methods

    internal void CommitInsert(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> indexFields)
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
    }

    internal void CommitUpdate(string collectionName, int docId, byte[] serializedData, IReadOnlyList<IndexFieldEntry> newIndexFields, IReadOnlyList<IndexFieldEntry> oldIndexFields)
    {
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation oldLocation = btree.Search(docId);

        if (oldIndexFields != null && oldIndexFields.Count > 0)
        {
            DeleteFromIndexesInternal(collection, docId, oldIndexFields);
        }

        if (oldLocation != null)
        {
            _documentStorage.DeleteDocument(oldLocation.PageId, oldLocation.SlotIndex);
        }

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
        DocumentLocation location = btree.Search(docId);

        if (location != null)
        {
            if (oldIndexFields != null && oldIndexFields.Count > 0)
            {
                DeleteFromIndexesInternal(collection, docId, oldIndexFields);
            }

            _documentStorage.DeleteDocument(location.PageId, location.SlotIndex);
            btree.Delete(docId);

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
}
