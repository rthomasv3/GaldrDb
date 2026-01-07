using System;
using System.IO;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Storage;

public class PageManager
{
    private readonly IPageIO _pageIO;
    private readonly int _pageSize;
    private readonly double _growthFactor;
    private readonly int _minimumExpansion;
    private HeaderPage _header;
    private Bitmap _bitmap;
    private FreeSpaceMap _fsm;

    public PageManager(IPageIO pageIO, int pageSize, double growthFactor = 2.0, int minimumExpansion = 16)
    {
        _pageIO = pageIO;
        _pageSize = pageSize;
        _growthFactor = growthFactor;
        _minimumExpansion = minimumExpansion;
    }

    public HeaderPage Header
    {
        get { return _header; }
    }

    public void Initialize()
    {
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
            PageSize = _pageSize,
            TotalPageCount = totalPages,
            BitmapStartPage = bitmapStartPage,
            BitmapPageCount = bitmapPageCount,
            FsmStartPage = fsmStartPage,
            FsmPageCount = fsmPageCount,
            CollectionsMetadataPage = collectionsMetadataPage,
            MmapHint = 1,
            LastCommitFrame = 0,
            WalChecksum = 0
        };

        byte[] headerBytes = _header.Serialize(_pageSize);
        _pageIO.WritePage(0, headerBytes);

        _bitmap = new Bitmap(_pageIO, bitmapStartPage, bitmapPageCount, totalPages, _pageSize);
        _bitmap.AllocatePage(0);
        _bitmap.AllocatePage(1);
        _bitmap.AllocatePage(2);
        _bitmap.AllocatePage(3);
        _bitmap.WriteToDisk();

        _fsm = new FreeSpaceMap(_pageIO, fsmStartPage, fsmPageCount, totalPages, _pageSize);
        _fsm.SetFreeSpaceLevel(0, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(1, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(2, FreeSpaceLevel.None);
        _fsm.SetFreeSpaceLevel(3, FreeSpaceLevel.None);
        _fsm.WriteToDisk();

        _pageIO.Flush();
    }

    public void Load()
    {
        byte[] headerBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(0, headerBuffer);
            _header = HeaderPage.Deserialize(headerBuffer);
        }
        finally
        {
            BufferPool.Return(headerBuffer);
        }

        if (_header.MagicNumber != PageConstants.MAGIC_NUMBER)
        {
            throw new InvalidDataException("Invalid magic number in header");
        }

        if (_header.Version != PageConstants.VERSION)
        {
            throw new InvalidDataException($"Unsupported version: {_header.Version}");
        }

        _bitmap = new Bitmap(_pageIO, _header.BitmapStartPage, _header.BitmapPageCount, _header.TotalPageCount, _header.PageSize);
        _bitmap.LoadFromDisk();

        _fsm = new FreeSpaceMap(_pageIO, _header.FsmStartPage, _header.FsmPageCount, _header.TotalPageCount, _header.PageSize);
        _fsm.LoadFromDisk();
    }

    public int AllocatePage()
    {
        int pageId = _bitmap.FindFreePage();

        if (pageId == -1)
        {
            Expand();
            pageId = _bitmap.FindFreePage();

            if (pageId == -1)
            {
                throw new InvalidOperationException("No free pages available after expansion");
            }
        }

        _bitmap.AllocatePage(pageId);
        _bitmap.WriteToDisk();

        int result = pageId;

        return result;
    }

    public int[] AllocatePages(int count)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than zero");
        }

        int[] pageIds = new int[count];
        int allocated = 0;

        while (allocated < count)
        {
            int pageId = _bitmap.FindFreePage();

            if (pageId == -1)
            {
                int needed = count - allocated;
                int expansionSize = Math.Max(_minimumExpansion, needed * 2);
                Expand(expansionSize);

                pageId = _bitmap.FindFreePage();

                if (pageId == -1)
                {
                    throw new InvalidOperationException("No free pages available after expansion");
                }
            }

            _bitmap.AllocatePage(pageId);
            pageIds[allocated] = pageId;
            allocated++;
        }

        _bitmap.WriteToDisk();

        int[] result = pageIds;

        return result;
    }

    public void DeallocatePage(int pageId)
    {
        _bitmap.DeallocatePage(pageId);
        _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.High);
        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();
    }

    public bool IsAllocated(int pageId)
    {
        bool result = _bitmap.IsAllocated(pageId);

        return result;
    }

    public FreeSpaceLevel GetFreeSpaceLevel(int pageId)
    {
        FreeSpaceLevel result = _fsm.GetFreeSpaceLevel(pageId);

        return result;
    }

    public void SetFreeSpaceLevel(int pageId, FreeSpaceLevel level)
    {
        _fsm.SetFreeSpaceLevel(pageId, level);
        _fsm.WriteToDisk();
    }

    public int FindPageWithSpace(FreeSpaceLevel minLevel)
    {
        int result = _fsm.FindPageWithSpace(minLevel);

        return result;
    }

    public void Flush()
    {
        _pageIO.Flush();
    }

    private void Expand(int? requestedPages = null)
    {
        int oldTotalPages = _header.TotalPageCount;
        int additionalPages = requestedPages ?? CalculateExpansionSize(oldTotalPages);
        int newTotalPages = oldTotalPages + additionalPages;

        int maxPagesPerBitmapPage = _pageSize * 8;
        int maxPagesPerFsmPage = _pageSize * 4;
        int bitmapCapacity = _header.BitmapPageCount * maxPagesPerBitmapPage;
        int fsmCapacity = _header.FsmPageCount * maxPagesPerFsmPage;

        if (newTotalPages > bitmapCapacity || newTotalPages > fsmCapacity)
        {
            throw new InvalidOperationException("File expansion would exceed bitmap/FSM capacity");
        }

        byte[] emptyPage = new byte[_pageSize];
        for (int pageId = oldTotalPages; pageId < newTotalPages; pageId++)
        {
            _pageIO.WritePage(pageId, emptyPage);
        }

        _header.TotalPageCount = newTotalPages;
        byte[] headerBytes = _header.Serialize(_pageSize);
        _pageIO.WritePage(0, headerBytes);

        _bitmap.Resize(newTotalPages);
        _fsm.Resize(newTotalPages);

        for (int pageId = oldTotalPages; pageId < newTotalPages; pageId++)
        {
            _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.High);
        }

        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();
    }

    private int CalculateExpansionSize(int currentPageCount)
    {
        int calculated = (int)(currentPageCount * (_growthFactor - 1.0));
        int result = Math.Max(_minimumExpansion, calculated);

        return result;
    }
}
