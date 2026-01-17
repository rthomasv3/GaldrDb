using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class DocumentStorage : IDisposable
{
    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly PageLockManager _pageLockManager;
    private readonly int _pageSize;
    private bool _disposed;

    public DocumentStorage(IPageIO pageIO, PageManager pageManager, int pageSize)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _pageLockManager = new PageLockManager();
        _pageSize = pageSize;
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

        int[] pageIds = IntArrayPool.Rent(pagesNeeded);
        DocumentLocation result;

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            _pageLockManager.AcquireWriteLock(pageId);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            try
            {
                _pageIO.ReadPage(pageId, pageBuffer);

                bool wasInitialized = IsPageInitialized(pageBuffer);
                if (wasInitialized)
                {
                    DocumentPage.DeserializeTo(pageBuffer, page, _pageSize);
                }
                else
                {
                    page.Reset(_pageSize);
                }

                if (!page.CanFit(documentSize))
                {
                    int requiredSpace = documentSize + SlotEntry.SINGLE_PAGE_SLOT_SIZE;

                    if (page.GetLogicalFreeSpace() >= requiredSpace)
                    {
                        page.Compact();
                    }

                    // Re-check after compaction - if still doesn't fit, allocate new page
                    if (!page.CanFit(documentSize))
                    {
                        // Release old page lock and acquire lock on new page
                        _pageLockManager.ReleaseWriteLock(pageId);
                        pageId = _pageManager.AllocatePage();
                        pageIds[0] = pageId;
                        _pageLockManager.AcquireWriteLock(pageId);
                        page.Reset(_pageSize);
                    }
                }

                int slotIndex = page.AddDocument(documentBytes, pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                _pageIO.WritePage(pageId, pageBuffer);

                UpdateFSMForDocumentPage(pageId, page);

                result = new DocumentLocation(pageId, slotIndex);
            }
            finally
            {
                _pageLockManager.ReleaseWriteLock(pageId);
                DocumentPagePool.Return(page);
                BufferPool.Return(pageBuffer);
            }
        }
        else
        {
            pageIds = _pageManager.AllocatePages(pagesNeeded);

            int firstPageId = pageIds[0];
            int offset = 0;
            int firstPageDataSize = Math.Min(documentSize, usablePageSize);

            // Acquire write locks on all pages (in order to avoid deadlocks)
            for (int i = 0; i < pagesNeeded; i++)
            {
                _pageLockManager.AcquireWriteLock(pageIds[i]);
            }

            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                page.Reset(_pageSize);
                int slotIndexResult = page.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                _pageIO.WritePage(firstPageId, pageBuffer);

                // For multi-page documents, use contiguous free space for FSM since
                // continuation pages are raw data buffers marked as FreeSpaceLevel.None
                UpdateFSM(firstPageId, page.GetFreeSpaceBytes());

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
                // Release all page locks
                for (int i = 0; i < pagesNeeded; i++)
                {
                    _pageLockManager.ReleaseWriteLock(pageIds[i]);
                }
                DocumentPagePool.Return(page);
                BufferPool.Return(pageBuffer);
            }
        }

        return result;
    }

    public byte[] ReadDocument(int pageId, int slotIndex)
    {
        byte[] result = null;
        int[] continuationPageIds = null;
        int continuationPageCount = 0;

        _pageLockManager.AcquireReadLock(pageId);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize);

            if (slotIndex < 0 || slotIndex >= page.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = page.Slots[slotIndex];

            if (entry.PageCount == 0 || entry.TotalSize == 0)
            {
                throw new InvalidOperationException("Document slot has been deleted");
            }

            if (entry.PageCount == 1)
            {
                result = page.GetDocumentData(slotIndex);
            }
            else
            {
                byte[] fullDocument = new byte[entry.TotalSize];
                int offset = 0;

                byte[] firstPageData = page.GetDocumentData(slotIndex);
                Array.Copy(firstPageData, 0, fullDocument, offset, firstPageData.Length);
                offset += firstPageData.Length;

                // Store continuation page IDs and acquire locks
                continuationPageCount = entry.PageCount - 1;
                continuationPageIds = new int[continuationPageCount];
                for (int i = 1; i < entry.PageCount; i++)
                {
                    continuationPageIds[i - 1] = entry.PageIds[i];
                    _pageLockManager.AcquireReadLock(entry.PageIds[i]);
                }

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
            _pageLockManager.ReleaseReadLock(pageId);
            if (continuationPageIds != null)
            {
                for (int i = 0; i < continuationPageCount; i++)
                {
                    _pageLockManager.ReleaseReadLock(continuationPageIds[i]);
                }
            }
            DocumentPagePool.Return(page);
            BufferPool.Return(pageBuffer);
        }

        return result;
    }

    public void DeleteDocument(int pageId, int slotIndex)
    {
        _pageLockManager.AcquireWriteLock(pageId);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize);

            if (slotIndex < 0 || slotIndex >= page.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = page.Slots[slotIndex];

            if (entry.PageCount > 1)
            {
                for (int i = 1; i < entry.PageCount; i++)
                {
                    int continuationPageId = entry.PageIds[i];
                    _pageManager.DeallocatePage(continuationPageId);
                }
            }

            if (entry.PageIds != null)
            {
                IntArrayPool.Return(entry.PageIds);
            }

            page.Slots[slotIndex] = new SlotEntry
            {
                PageCount = 0,
                PageIds = null,
                TotalSize = 0,
                Offset = 0,
                Length = 0
            };

            page.SerializeTo(pageBuffer);
            _pageIO.WritePage(pageId, pageBuffer);

            UpdateFSMForDocumentPage(pageId, page);
        }
        finally
        {
            _pageLockManager.ReleaseWriteLock(pageId);
            DocumentPagePool.Return(page);
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

        if (pageId == -1 || !_pageManager.IsAllocated(pageId) || pageId < PageConstants.FIRST_DATA_PAGE_ID)
        {
            pageId = _pageManager.AllocatePage();
        }

        return pageId;
    }

    private int FindCompactablePageForDocument(int requiredSpace)
    {
        int result = -1;
        int totalPages = _pageManager.Header.TotalPageCount;
        byte[] pageBuffer = BufferPool.Rent(_pageSize);

        try
        {
            for (int pageId = PageConstants.FIRST_DATA_PAGE_ID; pageId < totalPages && result == -1; pageId++)
            {
                if (_pageManager.IsAllocated(pageId))
                {
                    FreeSpaceLevel level = _pageManager.GetFreeSpaceLevel(pageId);
                    if (level != FreeSpaceLevel.High)
                    {
                        _pageIO.ReadPage(pageId, pageBuffer);

                        if (IsPageInitialized(pageBuffer))
                        {
                            int logicalFreeSpace = DocumentPage.GetLogicalFreeSpaceFromBuffer(pageBuffer, _pageSize);

                            if (logicalFreeSpace >= requiredSpace)
                            {
                                result = pageId;
                            }
                        }
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
        }

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

    private void UpdateFSMForDocumentPage(int pageId, DocumentPage page)
    {
        int logicalFreeSpace = page.GetLogicalFreeSpace();
        double freeSpacePercentage = (double)logicalFreeSpace / _pageSize;
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

        int[] pageIds = IntArrayPool.Rent(pagesNeeded);
        DocumentLocation result;

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            await _pageLockManager.AcquireWriteLockAsync(pageId, cancellationToken).ConfigureAwait(false);
            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                await _pageIO.ReadPageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                if (IsPageInitialized(pageBuffer))
                {
                    DocumentPage.DeserializeTo(pageBuffer, page, _pageSize);
                }
                else
                {
                    page.Reset(_pageSize);
                }

                if (!page.CanFit(documentSize))
                {
                    int requiredSpace = documentSize + SlotEntry.SINGLE_PAGE_SLOT_SIZE;

                    if (page.GetLogicalFreeSpace() >= requiredSpace)
                    {
                        page.Compact();
                    }

                    if (!page.CanFit(documentSize))
                    {
                        // Release old page lock and acquire lock on new page
                        _pageLockManager.ReleaseWriteLock(pageId);
                        pageId = _pageManager.AllocatePage();
                        pageIds[0] = pageId;
                        await _pageLockManager.AcquireWriteLockAsync(pageId, cancellationToken).ConfigureAwait(false);
                        page.Reset(_pageSize);
                    }
                }

                int slotIndex = page.AddDocument(documentBytes, pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                UpdateFSMForDocumentPage(pageId, page);

                result = new DocumentLocation(pageId, slotIndex);
            }
            finally
            {
                _pageLockManager.ReleaseWriteLock(pageId);
                DocumentPagePool.Return(page);
                BufferPool.Return(pageBuffer);
            }
        }
        else
        {
            pageIds = _pageManager.AllocatePages(pagesNeeded);

            int firstPageId = pageIds[0];
            int offset = 0;
            int firstPageDataSize = Math.Min(documentSize, usablePageSize);

            // Acquire write locks on all pages (in order to avoid deadlocks)
            for (int i = 0; i < pagesNeeded; i++)
            {
                await _pageLockManager.AcquireWriteLockAsync(pageIds[i], cancellationToken).ConfigureAwait(false);
            }

            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                page.Reset(_pageSize);
                int slotIndexResult = page.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(firstPageId, pageBuffer, cancellationToken).ConfigureAwait(false);

                // For multi-page documents, use contiguous free space for FSM since
                // continuation pages are raw data buffers marked as FreeSpaceLevel.None
                UpdateFSM(firstPageId, page.GetFreeSpaceBytes());

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
                // Release all page locks
                for (int i = 0; i < pagesNeeded; i++)
                {
                    _pageLockManager.ReleaseWriteLock(pageIds[i]);
                }
                DocumentPagePool.Return(page);
                BufferPool.Return(pageBuffer);
            }
        }

        return result;
    }

    public async Task<byte[]> ReadDocumentAsync(int pageId, int slotIndex, CancellationToken cancellationToken = default)
    {
        byte[] result = null;
        int[] continuationPageIds = null;
        int continuationPageCount = 0;

        await _pageLockManager.AcquireReadLockAsync(pageId, cancellationToken).ConfigureAwait(false);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, pageBuffer, cancellationToken).ConfigureAwait(false);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize);

            if (slotIndex < 0 || slotIndex >= page.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = page.Slots[slotIndex];

            if (entry.PageCount == 0 || entry.TotalSize == 0)
            {
                throw new InvalidOperationException("Document slot has been deleted");
            }

            if (entry.PageCount == 1)
            {
                result = page.GetDocumentData(slotIndex);
            }
            else
            {
                byte[] fullDocument = new byte[entry.TotalSize];
                int offset = 0;

                byte[] firstPageData = page.GetDocumentData(slotIndex);
                Array.Copy(firstPageData, 0, fullDocument, offset, firstPageData.Length);
                offset += firstPageData.Length;

                // Store continuation page IDs and acquire locks
                continuationPageCount = entry.PageCount - 1;
                continuationPageIds = new int[continuationPageCount];
                for (int i = 1; i < entry.PageCount; i++)
                {
                    continuationPageIds[i - 1] = entry.PageIds[i];
                    await _pageLockManager.AcquireReadLockAsync(entry.PageIds[i], cancellationToken).ConfigureAwait(false);
                }

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
            _pageLockManager.ReleaseReadLock(pageId);
            if (continuationPageIds != null)
            {
                for (int i = 0; i < continuationPageCount; i++)
                {
                    _pageLockManager.ReleaseReadLock(continuationPageIds[i]);
                }
            }
            DocumentPagePool.Return(page);
            BufferPool.Return(pageBuffer);
        }

        return result;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _pageLockManager.Dispose();
        }
    }
}
