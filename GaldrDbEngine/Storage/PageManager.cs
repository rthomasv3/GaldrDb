using System;
using System.IO;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class PageManager
{
    private IPageIO _pageIO;
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
        int collectionsMetadataStartPage = 3;
        int collectionsMetadataPageCount = 1;

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
            CollectionsMetadataStartPage = collectionsMetadataStartPage,
            CollectionsMetadataPageCount = collectionsMetadataPageCount,
            MmapHint = 1
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

    public void SetPageIO(IPageIO pageIO)
    {
        _pageIO = pageIO;
        _bitmap.SetPageIO(_pageIO);
        _fsm.SetPageIO(_pageIO);
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
        _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.None);
        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();

        return pageId;
    }

    public int[] AllocatePages(int count)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be greater than zero");
        }

        int[] pageIds = IntArrayPool.Rent(count);
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
            _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.None);
            pageIds[allocated] = pageId;
            allocated++;
        }

        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();

        return pageIds;
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
        return _bitmap.IsAllocated(pageId);
    }

    public FreeSpaceLevel GetFreeSpaceLevel(int pageId)
    {
        return _fsm.GetFreeSpaceLevel(pageId);
    }

    public void SetFreeSpaceLevel(int pageId, FreeSpaceLevel level)
    {
        _fsm.SetFreeSpaceLevel(pageId, level);
        _fsm.WriteToDisk();
    }

    public int FindPageWithSpace(FreeSpaceLevel minLevel)
    {
        return _fsm.FindPageWithSpace(minLevel);
    }

    public void Flush()
    {
        _pageIO.Flush();
    }

    public void GrowCollectionsMetadata(CollectionsMetadata collectionsMetadata, int additionalPagesNeeded)
    {
        int currentStartPage = _header.CollectionsMetadataStartPage;
        int currentPageCount = _header.CollectionsMetadataPageCount;
        int newPageCount = currentPageCount + additionalPagesNeeded;

        bool canExpandInPlace = true;
        for (int i = 0; i < additionalPagesNeeded; i++)
        {
            int pageToCheck = currentStartPage + currentPageCount + i;
            if (pageToCheck >= _header.TotalPageCount || _bitmap.IsAllocated(pageToCheck))
            {
                canExpandInPlace = false;
                break;
            }
        }

        if (canExpandInPlace)
        {
            for (int i = 0; i < additionalPagesNeeded; i++)
            {
                int pageId = currentStartPage + currentPageCount + i;
                _bitmap.AllocatePage(pageId);
                _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.None);
            }

            _header.CollectionsMetadataPageCount = newPageCount;

            byte[] headerBytes = _header.Serialize(_pageSize);
            _pageIO.WritePage(0, headerBytes);

            _bitmap.WriteToDisk();
            _fsm.WriteToDisk();

            collectionsMetadata.SetPageAllocation(currentStartPage, newPageCount);
        }
        else
        {
            int[] newPages = AllocatePages(newPageCount);
            try
            {
                int newStartPage = newPages[0];

                bool contiguous = true;
                for (int i = 1; i < newPageCount; i++)
                {
                    if (newPages[i] != newStartPage + i)
                    {
                        contiguous = false;
                        break;
                    }
                }

                if (!contiguous)
                {
                    for (int i = 0; i < newPageCount; i++)
                    {
                        DeallocatePage(newPages[i]);
                    }
                    throw new InvalidOperationException("Could not allocate contiguous pages for collections metadata growth");
                }

                byte[] pageBuffer = BufferPool.Rent(_pageSize);
                try
                {
                    for (int i = 0; i < currentPageCount; i++)
                    {
                        _pageIO.ReadPage(currentStartPage + i, pageBuffer);
                        _pageIO.WritePage(newStartPage + i, pageBuffer);
                    }
                }
                finally
                {
                    BufferPool.Return(pageBuffer);
                }

                for (int i = 0; i < currentPageCount; i++)
                {
                    _bitmap.DeallocatePage(currentStartPage + i);
                    _fsm.SetFreeSpaceLevel(currentStartPage + i, FreeSpaceLevel.High);
                }

                _header.CollectionsMetadataStartPage = newStartPage;
                _header.CollectionsMetadataPageCount = newPageCount;

                byte[] headerBytes = _header.Serialize(_pageSize);
                _pageIO.WritePage(0, headerBytes);

                _bitmap.WriteToDisk();
                _fsm.WriteToDisk();

                collectionsMetadata.SetPageAllocation(newStartPage, newPageCount);
            }
            finally
            {
                IntArrayPool.Return(newPages);
            }
        }
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

        // for (int pageId = oldTotalPages; pageId < newTotalPages; pageId++)
        // {
        //     _fsm.SetFreeSpaceLevel(pageId, FreeSpaceLevel.High);
        // }

        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();
    }

    private int CalculateExpansionSize(int currentPageCount)
    {
        int calculated = (int)(currentPageCount * (_growthFactor - 1.0));
        return Math.Max(_minimumExpansion, calculated);
    }
}
