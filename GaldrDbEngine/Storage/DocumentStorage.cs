using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

public class DocumentStorage : IDisposable
{
    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly int _pageSize;
    private readonly ThreadLocal<DocumentPage> _reusablePage;
    private bool _disposed;

    public DocumentStorage(IPageIO pageIO, PageManager pageManager, int pageSize)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _pageSize = pageSize;
        _reusablePage = new ThreadLocal<DocumentPage>(() => new DocumentPage());
    }

    public DocumentLocation WriteDocument(byte[] documentBytes)
    {
        int documentSize = documentBytes.Length;
        int usablePageSize = _pageSize - 100;
        int pagesNeeded = 1;

        if (documentSize > usablePageSize)
        {
            pagesNeeded = (documentSize + usablePageSize - 1) / usablePageSize;
        }

        int[] pageIds = new int[pagesNeeded];
        DocumentLocation result;

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                _pageIO.ReadPage(pageId, pageBuffer);

                bool wasInitialized = IsPageInitialized(pageBuffer);
                if (wasInitialized)
                {
                    DocumentPage.DeserializeTo(pageBuffer, _reusablePage.Value, _pageSize);
                }
                else
                {
                    _reusablePage.Value.Reset(_pageSize);
                }

                int slotIndex = _reusablePage.Value.AddDocument(documentBytes, pageIds, documentSize);

                _reusablePage.Value.SerializeTo(pageBuffer);
                _pageIO.WritePage(pageId, pageBuffer);

                UpdateFSM(pageId, _reusablePage.Value.GetFreeSpaceBytes());

                result = new DocumentLocation(pageId, slotIndex);
            }
            finally
            {
                BufferPool.Return(pageBuffer);
            }
        }
        else
        {
            pageIds = _pageManager.AllocatePages(pagesNeeded);

            int firstPageId = pageIds[0];
            int offset = 0;
            int firstPageDataSize = Math.Min(documentSize, usablePageSize);

            _reusablePage.Value.Reset(_pageSize);
            int slotIndexResult = _reusablePage.Value.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, documentSize);

            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                _reusablePage.Value.SerializeTo(pageBuffer);
                _pageIO.WritePage(firstPageId, pageBuffer);

                UpdateFSM(firstPageId, _reusablePage.Value.GetFreeSpaceBytes());

                offset += firstPageDataSize;

                for (int i = 1; i < pagesNeeded; i++)
                {
                    int pageId = pageIds[i];
                    int remainingSize = documentSize - offset;
                    int chunkSize = Math.Min(remainingSize, _pageSize);

                    Array.Clear(pageBuffer, 0, _pageSize);
                    Array.Copy(documentBytes, offset, pageBuffer, 0, chunkSize);

                    _pageIO.WritePage(pageId, pageBuffer);

                    // Continuation pages are raw data buffers, not slotted pages.
                    // Mark as no free space so they won't be found by FindOrAllocatePageForDocument.
                    _pageManager.SetFreeSpaceLevel(pageId, FreeSpaceLevel.None);

                    offset += chunkSize;
                }

                result = new DocumentLocation(firstPageId, slotIndexResult);
            }
            finally
            {
                BufferPool.Return(pageBuffer);
            }
        }

        return result;
    }

    public byte[] ReadDocument(int pageId, int slotIndex)
    {
        byte[] result = null;
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer);
            DocumentPage.DeserializeTo(pageBuffer, _reusablePage.Value, _pageSize);

            if (slotIndex < 0 || slotIndex >= _reusablePage.Value.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = _reusablePage.Value.Slots[slotIndex];

            if (entry.PageCount == 0 || entry.TotalSize == 0)
            {
                throw new InvalidOperationException("Document slot has been deleted");
            }

            if (entry.PageCount == 1)
            {
                result = _reusablePage.Value.GetDocumentData(slotIndex);
            }
            else
            {
                byte[] fullDocument = new byte[entry.TotalSize];
                int offset = 0;

                byte[] firstPageData = _reusablePage.Value.GetDocumentData(slotIndex);
                Array.Copy(firstPageData, 0, fullDocument, offset, firstPageData.Length);
                offset += firstPageData.Length;

                for (int i = 1; i < entry.PageCount; i++)
                {
                    int continuationPageId = entry.PageIds[i];
                    _pageIO.ReadPage(continuationPageId, pageBuffer);
                    int remainingSize = entry.TotalSize - offset;
                    int chunkSize = Math.Min(remainingSize, _pageSize);

                    Array.Copy(pageBuffer, 0, fullDocument, offset, chunkSize);
                    offset += chunkSize;
                }

                result = fullDocument;
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
        }

        return result;
    }

    public void DeleteDocument(int pageId, int slotIndex)
    {
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer);
            DocumentPage.DeserializeTo(pageBuffer, _reusablePage.Value, _pageSize);

            if (slotIndex < 0 || slotIndex >= _reusablePage.Value.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = _reusablePage.Value.Slots[slotIndex];

            if (entry.PageCount > 1)
            {
                for (int i = 1; i < entry.PageCount; i++)
                {
                    int continuationPageId = entry.PageIds[i];
                    _pageManager.DeallocatePage(continuationPageId);
                }
            }

            _reusablePage.Value.Slots[slotIndex] = new SlotEntry
            {
                PageCount = 0,
                PageIds = null,
                TotalSize = 0,
                Offset = 0,
                Length = 0
            };

            _reusablePage.Value.SerializeTo(pageBuffer);
            _pageIO.WritePage(pageId, pageBuffer);

            UpdateFSM(pageId, _reusablePage.Value.GetFreeSpaceBytes());
        }
        finally
        {
            BufferPool.Return(pageBuffer);
        }
    }

    private int FindOrAllocatePageForDocument(int documentSize)
    {
        int slotOverhead = 100;
        int requiredSpace = documentSize + slotOverhead;
        double requiredPercentage = (double)requiredSpace / _pageSize;
        FreeSpaceLevel minLevel = FreeSpaceLevel.None;

        if (requiredPercentage <= 0.30)
        {
            minLevel = FreeSpaceLevel.Low;
        }
        else if (requiredPercentage <= 0.60)
        {
            minLevel = FreeSpaceLevel.Medium;
        }
        else
        {
            minLevel = FreeSpaceLevel.High;
        }

        int pageId = _pageManager.FindPageWithSpace(minLevel);

        if (pageId == -1 || !_pageManager.IsAllocated(pageId) || pageId < 4)
        {
            pageId = _pageManager.AllocatePage();
        }

        int result = pageId;

        return result;
    }

    private void UpdateFSM(int pageId, int freeSpaceBytes)
    {
        double freeSpacePercentage = (double)freeSpaceBytes / _pageSize;
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
        else
        {
            level = FreeSpaceLevel.None;
        }

        _pageManager.SetFreeSpaceLevel(pageId, level);
    }

    private bool IsPageInitialized(byte[] pageBytes)
    {
        bool result = pageBytes[0] == PageConstants.PAGE_TYPE_DOCUMENT;

        return result;
    }

    public async Task<DocumentLocation> WriteDocumentAsync(byte[] documentBytes, CancellationToken cancellationToken = default)
    {
        int documentSize = documentBytes.Length;
        int usablePageSize = _pageSize - 100;
        int pagesNeeded = 1;

        if (documentSize > usablePageSize)
        {
            pagesNeeded = (documentSize + usablePageSize - 1) / usablePageSize;
        }

        int[] pageIds = new int[pagesNeeded];
        DocumentLocation result;

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                await _pageIO.ReadPageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                if (IsPageInitialized(pageBuffer))
                {
                    DocumentPage.DeserializeTo(pageBuffer, _reusablePage.Value, _pageSize);
                }
                else
                {
                    _reusablePage.Value.Reset(_pageSize);
                }

                int slotIndex = _reusablePage.Value.AddDocument(documentBytes, pageIds, documentSize);

                _reusablePage.Value.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                UpdateFSM(pageId, _reusablePage.Value.GetFreeSpaceBytes());

                result = new DocumentLocation(pageId, slotIndex);
            }
            finally
            {
                BufferPool.Return(pageBuffer);
            }
        }
        else
        {
            pageIds = _pageManager.AllocatePages(pagesNeeded);

            int firstPageId = pageIds[0];
            int offset = 0;
            int firstPageDataSize = Math.Min(documentSize, usablePageSize);

            _reusablePage.Value.Reset(_pageSize);
            int slotIndexResult = _reusablePage.Value.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, documentSize);

            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                _reusablePage.Value.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(firstPageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                UpdateFSM(firstPageId, _reusablePage.Value.GetFreeSpaceBytes());

                offset += firstPageDataSize;

                for (int i = 1; i < pagesNeeded; i++)
                {
                    int pageId = pageIds[i];
                    int remainingSize = documentSize - offset;
                    int chunkSize = Math.Min(remainingSize, _pageSize);

                    Array.Clear(pageBuffer, 0, _pageSize);
                    Array.Copy(documentBytes, offset, pageBuffer, 0, chunkSize);

                    await _pageIO.WritePageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                    // Continuation pages are raw data buffers, not slotted pages.
                    // Mark as no free space so they won't be found by FindOrAllocatePageForDocument.
                    _pageManager.SetFreeSpaceLevel(pageId, FreeSpaceLevel.None);

                    offset += chunkSize;
                }

                result = new DocumentLocation(firstPageId, slotIndexResult);
            }
            finally
            {
                BufferPool.Return(pageBuffer);
            }
        }

        return result;
    }

    public async Task<byte[]> ReadDocumentAsync(int pageId, int slotIndex, CancellationToken cancellationToken = default)
    {
        byte[] result = null;
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);
            DocumentPage.DeserializeTo(pageBuffer, _reusablePage.Value, _pageSize);

            if (slotIndex < 0 || slotIndex >= _reusablePage.Value.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = _reusablePage.Value.Slots[slotIndex];

            if (entry.PageCount == 0 || entry.TotalSize == 0)
            {
                throw new InvalidOperationException("Document slot has been deleted");
            }

            if (entry.PageCount == 1)
            {
                result = _reusablePage.Value.GetDocumentData(slotIndex);
            }
            else
            {
                byte[] fullDocument = new byte[entry.TotalSize];
                int offset = 0;

                byte[] firstPageData = _reusablePage.Value.GetDocumentData(slotIndex);
                Array.Copy(firstPageData, 0, fullDocument, offset, firstPageData.Length);
                offset += firstPageData.Length;

                for (int i = 1; i < entry.PageCount; i++)
                {
                    int continuationPageId = entry.PageIds[i];
                    await _pageIO.ReadPageAsync(continuationPageId, pageBuffer, cancellationToken).ConfigureAwait(false);
                    int remainingSize = entry.TotalSize - offset;
                    int chunkSize = Math.Min(remainingSize, _pageSize);

                    Array.Copy(pageBuffer, 0, fullDocument, offset, chunkSize);
                    offset += chunkSize;
                }

                result = fullDocument;
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
        }

        return result;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _reusablePage.Dispose();
            _disposed = true;
        }
    }
}
