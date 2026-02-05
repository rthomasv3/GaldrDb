using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class DocumentStorage : IDisposable
{
    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly PageLockManager _pageLockManager;
    private readonly int _pageSize;
    private readonly int _usablePageSize;
    private bool _disposed;

    public DocumentStorage(IPageIO pageIO, PageManager pageManager, PageLockManager pageLockManager, int pageSize, int usablePageSize = 0)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _pageLockManager = pageLockManager;
        _pageSize = pageSize;
        _usablePageSize = usablePageSize > 0 ? usablePageSize : pageSize;
    }

    public DocumentLocation WriteDocument(byte[] documentBytes, TransactionContext context = null)
    {
        int documentSize = documentBytes.Length;
        int usablePageSizeForData = _usablePageSize - DocumentPage.DOCUMENT_PAGE_OVERHEAD;
        int pagesNeeded = 1;

        if (documentSize > usablePageSizeForData)
        {
            pagesNeeded = (documentSize + usablePageSizeForData - 1) / usablePageSizeForData;
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
                _pageIO.ReadPage(pageId, pageBuffer, context);

                if (IsPageInitialized(pageBuffer))
                {
                    DocumentPage.DeserializeTo(pageBuffer, page, _pageSize, _usablePageSize);
                }
                else
                {
                    page.Reset(_pageSize, _usablePageSize);
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
                        page.Reset(_pageSize, _usablePageSize);
                    }
                }

                int slotIndex = page.AddDocument(documentBytes, pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                _pageIO.WritePage(pageId, pageBuffer, context);

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
            int firstPageDataSize = Math.Min(documentSize, usablePageSizeForData);

            // Acquire write locks on all pages (in order to avoid deadlocks)
            for (int i = 0; i < pagesNeeded; i++)
            {
                _pageLockManager.AcquireWriteLock(pageIds[i]);
            }

            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                page.Reset(_pageSize, _usablePageSize);
                int slotIndexResult = page.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                _pageIO.WritePage(firstPageId, pageBuffer, context);

                // For multi-page documents, use contiguous free space for FSM since
                // continuation pages are raw data buffers marked as FreeSpaceLevel.None
                UpdateFSM(firstPageId, page.GetFreeSpaceBytes());

                offset += firstPageDataSize;

                for (int i = 1; i < pagesNeeded; i++)
                {
                    int pageId = pageIds[i];
                    int remainingSize = documentSize - offset;
                    int chunkSize = Math.Min(remainingSize, _usablePageSize);

                    Array.Clear(pageBuffer, 0, _pageSize);
                    Array.Copy(documentBytes, offset, pageBuffer, 0, chunkSize);

                    _pageIO.WritePage(pageId, pageBuffer, context);

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

    public byte[] ReadDocument(int pageId, int slotIndex, TransactionContext context = null)
    {
        byte[] result = null;
        int[] continuationPageIds = null;
        int continuationPageCount = 0;
        DocumentPage page = null;

        _pageLockManager.AcquireReadLock(pageId);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer, context);

            // Use fast path for single-page documents (avoids 8KB PageData copy)
            ReadDocumentResult readResult = DocumentPage.ReadDocumentFromBuffer(pageBuffer, slotIndex, pageId);
            SlotEntry entry = readResult.Slot;

            if (entry.PageCount == 1)
            {
                result = readResult.DocumentData;
            }
            else
            {
                // Multi-page document: need full deserialization for continuation pages
                byte[] fullDocument = new byte[entry.TotalSize];
                int offset = 0;

                Array.Copy(readResult.DocumentData, 0, fullDocument, offset, readResult.DocumentData.Length);
                offset += readResult.DocumentData.Length;

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
                    _pageIO.ReadPage(continuationPageId, pageBuffer, context);
                    int remainingSize = entry.TotalSize - offset;
                    int chunkSize = Math.Min(remainingSize, _usablePageSize);

                    Array.Copy(pageBuffer, 0, fullDocument, offset, chunkSize);
                    offset += chunkSize;
                }

                result = fullDocument;
            }

            // Return the pooled int array from the slot
            if (entry.PageIds != null)
            {
                IntArrayPool.Return(entry.PageIds);
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
            if (page != null)
            {
                DocumentPagePool.Return(page);
            }
            BufferPool.Return(pageBuffer);
        }

        return result;
    }

    public void DeleteDocument(int pageId, int slotIndex, TransactionContext context = null)
    {
        _pageLockManager.AcquireWriteLock(pageId);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer, context);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize, _usablePageSize);

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
            _pageIO.WritePage(pageId, pageBuffer, context);

            UpdateFSMForDocumentPage(pageId, page);
        }
        finally
        {
            _pageLockManager.ReleaseWriteLock(pageId);
            DocumentPagePool.Return(page);
            BufferPool.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Attempts to delete a document. Returns false if slot no longer exists (already deleted/compacted).
    /// Used by GC to gracefully handle stale slot indices.
    /// </summary>
    public bool TryDeleteDocument(int pageId, int slotIndex, TransactionContext context = null)
    {
        bool deleted = false;

        _pageLockManager.AcquireWriteLock(pageId);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, pageBuffer, context);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize, _usablePageSize);

            // Graceful handling - slot may have been compacted/deleted
            if (slotIndex >= 0 && slotIndex < page.SlotCount)
            {
                SlotEntry entry = page.Slots[slotIndex];

                // Check if slot is already deleted
                if (entry.PageCount > 0)
                {
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
                    _pageIO.WritePage(pageId, pageBuffer, context);

                    UpdateFSMForDocumentPage(pageId, page);
                    deleted = true;
                }
            }
        }
        finally
        {
            _pageLockManager.ReleaseWriteLock(pageId);
            DocumentPagePool.Return(page);
            BufferPool.Return(pageBuffer);
        }

        return deleted;
    }

    private int FindOrAllocatePageForDocument(int documentSize)
    {
        int slotOverhead = DocumentPage.DOCUMENT_PAGE_OVERHEAD;
        int requiredSpace = documentSize + slotOverhead;
        double requiredPercentage = (double)requiredSpace / _usablePageSize;
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

    private int FindCompactablePageForDocument(int requiredSpace, TransactionContext context = null)
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
                        _pageIO.ReadPage(pageId, pageBuffer, context);

                        if (IsPageInitialized(pageBuffer))
                        {
                            int logicalFreeSpace = DocumentPage.GetLogicalFreeSpaceFromBuffer(pageBuffer, _usablePageSize);

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
        double freeSpacePercentage = (double)freeSpaceBytes / _usablePageSize;
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
        double freeSpacePercentage = (double)logicalFreeSpace / _usablePageSize;
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
        return pageBytes[0] == PageConstants.PAGE_TYPE_DOCUMENT;
    }

    public async Task<DocumentLocation> WriteDocumentAsync(byte[] documentBytes, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        int documentSize = documentBytes.Length;
        int usablePageSizeForData = _usablePageSize - DocumentPage.DOCUMENT_PAGE_OVERHEAD;
        int pagesNeeded = 1;

        if (documentSize > usablePageSizeForData)
        {
            pagesNeeded = (documentSize + usablePageSizeForData - 1) / usablePageSizeForData;
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
                await _pageIO.ReadPageAsync(pageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);

                if (IsPageInitialized(pageBuffer))
                {
                    DocumentPage.DeserializeTo(pageBuffer, page, _pageSize, _usablePageSize);
                }
                else
                {
                    page.Reset(_pageSize, _usablePageSize);
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
                        page.Reset(_pageSize, _usablePageSize);
                    }
                }

                int slotIndex = page.AddDocument(documentBytes, pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(pageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);

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
            int firstPageDataSize = Math.Min(documentSize, usablePageSizeForData);

            // Acquire write locks on all pages (in order to avoid deadlocks)
            for (int i = 0; i < pagesNeeded; i++)
            {
                await _pageLockManager.AcquireWriteLockAsync(pageIds[i], cancellationToken).ConfigureAwait(false);
            }

            DocumentPage page = DocumentPagePool.Rent(_pageSize);
            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                page.Reset(_pageSize, _usablePageSize);
                int slotIndexResult = page.AddDocument(documentBytes.AsSpan(0, firstPageDataSize), pageIds, pagesNeeded, documentSize);

                page.SerializeTo(pageBuffer);
                await _pageIO.WritePageAsync(firstPageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);

                // For multi-page documents, use contiguous free space for FSM since
                // continuation pages are raw data buffers marked as FreeSpaceLevel.None
                UpdateFSM(firstPageId, page.GetFreeSpaceBytes());

                offset += firstPageDataSize;

                for (int i = 1; i < pagesNeeded; i++)
                {
                    int pageId = pageIds[i];
                    int remainingSize = documentSize - offset;
                    int chunkSize = Math.Min(remainingSize, _usablePageSize);

                    Array.Clear(pageBuffer, 0, _pageSize);
                    Array.Copy(documentBytes, offset, pageBuffer, 0, chunkSize);

                    await _pageIO.WritePageAsync(pageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);

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

    public async Task<byte[]> ReadDocumentAsync(int pageId, int slotIndex, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        byte[] result = null;
        int[] continuationPageIds = null;
        int continuationPageCount = 0;

        await _pageLockManager.AcquireReadLockAsync(pageId, cancellationToken).ConfigureAwait(false);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);
        DocumentPage page = DocumentPagePool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);
            DocumentPage.DeserializeTo(pageBuffer, page, _pageSize, _usablePageSize);

            if (slotIndex < 0 || slotIndex >= page.SlotCount)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex));
            }

            SlotEntry entry = page.Slots[slotIndex];

            if (entry.PageCount == 0 || entry.TotalSize == 0)
            {
                throw new DocumentSlotDeletedException(pageId, slotIndex);
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
                    await _pageIO.ReadPageAsync(continuationPageId, pageBuffer, context, cancellationToken).ConfigureAwait(false);
                    int remainingSize = entry.TotalSize - offset;
                    int chunkSize = Math.Min(remainingSize, _usablePageSize);

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
        }
    }
}
