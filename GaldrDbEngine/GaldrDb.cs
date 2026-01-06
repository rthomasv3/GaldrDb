using System;
using System.IO;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Storage;

namespace GaldrDbEngine;

public class GaldrDb : IDisposable
{
    #region Fields

    private readonly string _filePath;
    private readonly GaldrDbOptions _options;
    private readonly string _walPath;
    private IPageIO _pageIO;
    private HeaderPage _header;
    private Bitmap _bitmap;
    private FreeSpaceMap _fsm;
    private DocumentStorage _documentStorage;

    #endregion

    #region Constructor

    public GaldrDb(string filePath, GaldrDbOptions options)
    {
        _filePath = filePath;
        _options = options ?? new GaldrDbOptions();
        _walPath = $"{Path.GetFileNameWithoutExtension(filePath)}.db-wal";
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
        var db = new GaldrDb(filePath, options);
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

    /// <summary>
    /// Create and write initial structure
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    private void InitializeFile()
    {
        if (File.Exists(_filePath))
        {
            throw new InvalidOperationException("File exists; use Open.");
        }

        SetupIO();

        int pageSize = _options.PageSize;
        int totalPages = 4;
        int bitmapStartPage = 1;
        int bitmapPageCount = 1;
        int fsmStartPage = 2;
        int fsmPageCount = 1;
        int collectionsMetadataPage = 3;

        _header = new HeaderPage
        {
            MagicNumber = PageConstants.MAGIC_NUMBER,
            Version = PageConstants.VERSION,
            PageSize = pageSize,
            TotalPageCount = totalPages,
            BitmapStartPage = bitmapStartPage,
            BitmapPageCount = bitmapPageCount,
            FsmStartPage = fsmStartPage,
            FsmPageCount = fsmPageCount,
            CollectionsMetadataPage = collectionsMetadataPage,
            MmapHint = (byte)(_options.UseMmap ? 1 : 0),
            LastCommitFrame = 0,
            WalChecksum = 0
        };

        byte[] headerBytes = _header.Serialize(pageSize);
        _pageIO.WritePage(0, headerBytes);

        _bitmap = new Bitmap(_pageIO, bitmapStartPage, bitmapPageCount, totalPages);
        _bitmap.AllocatePage(0);
        _bitmap.AllocatePage(1);
        _bitmap.AllocatePage(2);
        _bitmap.AllocatePage(3);
        _bitmap.WriteToDisk();

        _fsm = new FreeSpaceMap(_pageIO, fsmStartPage, fsmPageCount, totalPages);
        _fsm.SetFreeSpaceLevel(0, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(1, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(2, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(3, FreeSpaceLevel.None);
        _fsm.WriteToDisk();

        byte[] emptyCollectionsPage = new byte[pageSize];
        _pageIO.WritePage(collectionsMetadataPage, emptyCollectionsPage);

        _pageIO.Flush();

        _documentStorage = new DocumentStorage(_pageIO, _bitmap, _fsm, pageSize);

        if (_options.UseWal)
        {
            InitializeWalFile();
        }
    }

    /// <summary>
    /// Read, validate, recover WAL
    /// </summary>
    /// <exception cref="FileNotFoundException"></exception>
    private void OpenAndValidateFile()
    {
        if (!File.Exists(_filePath))
        {
            throw new FileNotFoundException();
        }

        SetupIO();

        byte[] headerBytes = _pageIO.ReadPage(0);
        _header = HeaderPage.Deserialize(headerBytes);

        if (_header.MagicNumber != PageConstants.MAGIC_NUMBER)
        {
            throw new InvalidDataException("Invalid magic number in header");
        }

        if (_header.Version != PageConstants.VERSION)
        {
            throw new InvalidDataException($"Unsupported version: {_header.Version}");
        }

        if (_options.PageSize == 0 || _options.PageSize != _header.PageSize)
        {
            _options.PageSize = _header.PageSize;
        }

        _bitmap = new Bitmap(_pageIO, _header.BitmapStartPage, _header.BitmapPageCount, _header.TotalPageCount);
        _bitmap.LoadFromDisk();

        _fsm = new FreeSpaceMap(_pageIO, _header.FsmStartPage, _header.FsmPageCount, _header.TotalPageCount);
        _fsm.LoadFromDisk();

        _documentStorage = new DocumentStorage(_pageIO, _bitmap, _fsm, _header.PageSize);

        if (_options.UseWal && File.Exists(_walPath))
        {
            RecoverFromWal();
        }
    }

    /// <summary>
    /// Setup I/O (mmap if enabled/supported)
    /// </summary>
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

    #endregion
}