using System;
using System.IO;
using System.Text;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Storage;
using GaldrDbCore.Utilities;
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

    #endregion
}
