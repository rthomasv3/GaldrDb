using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;
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
    private IPageIO _pageIO;
    private PageManager _pageManager;
    private DocumentStorage _documentStorage;
    private CollectionsMetadata _collectionsMetadata;

    #endregion

    #region Constructor

    public GaldrDb(string filePath, GaldrDbOptions options)
    {
        _filePath = filePath;
        _options = options ?? new GaldrDbOptions();
        _walPath = $"{Path.GetFileNameWithoutExtension(filePath)}.db-wal";
        _jsonSerializer = new GaldrJsonSerializer();
        _jsonOptions = new GaldrJsonOptions()
        {
            PropertyNameCaseInsensitive = false,
            PropertyNamingPolicy = PropertyNamingPolicy.Exact,
            WriteIndented = false,
            DetectCycles = true,
        };
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
        if (_pageIO != null)
        {
            _pageIO.Close();
            _pageIO.Dispose();
        }
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

    public void EnsureCollection<T>(GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);

        if (collection == null)
        {
            CreateCollection(collectionName);
            collection = _collectionsMetadata.FindCollection(collectionName);
        }

        EnsureIndexes(collection, typeInfo);
    }

    public int Insert<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;

        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist. Call EnsureCollection<T>() first.");
        }

        int currentId = typeInfo.IdGetter(document);
        int assignedId;

        if (currentId == 0)
        {
            assignedId = collection.NextId;
            typeInfo.IdSetter(document, assignedId);
        }
        else
        {
            assignedId = currentId;
        }

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
        DocumentLocation location = _documentStorage.WriteDocument(jsonBytes);

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        btree.Insert(assignedId, location);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        InsertIntoIndexes(collection, document, assignedId, location, typeInfo);

        collection.DocumentCount++;
        if (assignedId >= collection.NextId)
        {
            collection.NextId = assignedId + 1;
        }
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return assignedId;
    }

    public T GetById<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;
        T result = GetDocument<T>(collectionName, id);
        return result;
    }

    public bool Update<T>(T document, GaldrTypeInfo<T> typeInfo)
    {
        int id = typeInfo.IdGetter(document);

        if (id == 0)
        {
            throw new InvalidOperationException("Cannot update a document with Id = 0. The document must have a valid Id.");
        }

        string collectionName = typeInfo.CollectionName;
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation oldLocation = btree.Search(id);

        if (oldLocation == null)
        {
            return false;
        }

        T oldDocument = default(T);
        if (collection.Indexes.Count > 0)
        {
            byte[] oldJsonBytes = _documentStorage.ReadDocument(oldLocation.PageId, oldLocation.SlotIndex);
            string oldJson = Encoding.UTF8.GetString(oldJsonBytes);
            oldDocument = _jsonSerializer.Deserialize<T>(oldJson, _jsonOptions);
            DeleteFromIndexes(collection, oldDocument, id, typeInfo);
        }

        _documentStorage.DeleteDocument(oldLocation.PageId, oldLocation.SlotIndex);

        string json = _jsonSerializer.Serialize(document, _jsonOptions);
        byte[] jsonBytes = Encoding.UTF8.GetBytes(json);
        DocumentLocation newLocation = _documentStorage.WriteDocument(jsonBytes);

        btree.Delete(id);
        btree.Insert(id, newLocation);

        int newRootPageId = btree.GetRootPageId();
        if (newRootPageId != collection.RootPage)
        {
            collection.RootPage = newRootPageId;
        }

        InsertIntoIndexes(collection, document, id, newLocation, typeInfo);

        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return true;
    }

    public bool Delete<T>(int id, GaldrTypeInfo<T> typeInfo)
    {
        string collectionName = typeInfo.CollectionName;
        CollectionEntry collection = _collectionsMetadata.FindCollection(collectionName);
        if (collection == null)
        {
            throw new InvalidOperationException($"Collection '{collectionName}' does not exist");
        }

        int order = CalculateBTreeOrder(_options.PageSize);
        BTree btree = new BTree(_pageIO, _pageManager, collection.RootPage, _options.PageSize, order);
        DocumentLocation location = btree.Search(id);

        if (location == null)
        {
            return false;
        }

        if (collection.Indexes.Count > 0)
        {
            byte[] jsonBytes = _documentStorage.ReadDocument(location.PageId, location.SlotIndex);
            string json = Encoding.UTF8.GetString(jsonBytes);
            T document = _jsonSerializer.Deserialize<T>(json, _jsonOptions);
            DeleteFromIndexes(collection, document, id, typeInfo);
        }

        _documentStorage.DeleteDocument(location.PageId, location.SlotIndex);
        btree.Delete(id);

        collection.DocumentCount--;
        _collectionsMetadata.UpdateCollection(collection);
        _collectionsMetadata.WriteToDisk();

        return true;
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

    private void InsertIntoIndexes<T>(CollectionEntry collection, T document, int docId, DocumentLocation location, GaldrTypeInfo<T> typeInfo)
    {
        if (collection.Indexes.Count == 0)
        {
            return;
        }

        IndexFieldWriter writer = new IndexFieldWriter();
        typeInfo.ExtractIndexedFields(document, writer);
        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();

        // First pass: check unique constraints
        foreach ((string FieldName, byte[] KeyBytes) field in fields)
        {
            IndexDefinition indexDef = collection.FindIndex(field.FieldName);
            if (indexDef != null && indexDef.IsUnique)
            {
                CheckUniqueConstraint(indexDef, field.FieldName, field.KeyBytes);
            }
        }

        // Second pass: insert into all indexes
        foreach ((string FieldName, byte[] KeyBytes) field in fields)
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

    private void DeleteFromIndexes<T>(CollectionEntry collection, T document, int docId, GaldrTypeInfo<T> typeInfo)
    {
        if (collection.Indexes.Count == 0)
        {
            return;
        }

        IndexFieldWriter writer = new IndexFieldWriter();
        typeInfo.ExtractIndexedFields(document, writer);
        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();

        foreach ((string FieldName, byte[] KeyBytes) field in fields)
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

        _pageManager = new PageManager(_pageIO, _options.PageSize);
        _pageManager.Initialize();

        _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataPage, _options.PageSize);
        _collectionsMetadata.WriteToDisk();

        _pageManager.Flush();

        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _options.PageSize);

        if (_options.UseWal)
        {
            InitializeWalFile();
        }
    }

    private void OpenAndValidateFile()
    {
        if (!File.Exists(_filePath))
        {
            throw new FileNotFoundException();
        }

        SetupIO();

        _pageManager = new PageManager(_pageIO, _options.PageSize);
        _pageManager.Load();

        if (_options.PageSize == 0 || _options.PageSize != _pageManager.Header.PageSize)
        {
            _options.PageSize = _pageManager.Header.PageSize;
        }

        _collectionsMetadata = new CollectionsMetadata(_pageIO, _pageManager.Header.CollectionsMetadataPage, _pageManager.Header.PageSize);
        _collectionsMetadata.LoadFromDisk();

        _documentStorage = new DocumentStorage(_pageIO, _pageManager, _pageManager.Header.PageSize);

        if (_options.UseWal && File.Exists(_walPath))
        {
            RecoverFromWal();
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
        IPageIO pageIO = null;

        if (useMmap)
        {
            pageIO = new MmapPageIO(_filePath, _options.PageSize, initialSize, createNew);
        }
        else
        {
            pageIO = new StandardPageIO(_filePath, _options.PageSize, createNew);
        }

        _pageIO = pageIO;
    }

    private void InitializeWalFile()
    {

    }

    private void RecoverFromWal()
    {

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

    #endregion
}
