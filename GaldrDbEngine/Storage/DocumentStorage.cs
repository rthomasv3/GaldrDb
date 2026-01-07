using System;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Storage;

public class DocumentStorage
{
    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly int _pageSize;

    public DocumentStorage(IPageIO pageIO, PageManager pageManager, int pageSize)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
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

        int[] pageIds = new int[pagesNeeded];
        DocumentLocation result = null;

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            byte[] pageBuffer = BufferPool.Rent(_pageSize);
            try
            {
                _pageIO.ReadPage(pageId, pageBuffer);
                DocumentPage page = null;

                if (IsPageInitialized(pageBuffer))
                {
                    page = DocumentPage.Deserialize(pageBuffer, _pageSize);
                }
                else
                {
                    page = DocumentPage.CreateNew(_pageSize);
                }

                int slotIndex = page.AddDocument(documentBytes, pageIds, documentSize);

                byte[] serializedPage = page.Serialize();
                _pageIO.WritePage(pageId, serializedPage);

                UpdateFSM(pageId, page.GetFreeSpaceBytes());

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

            byte[] firstPageData = new byte[firstPageDataSize];
            Array.Copy(documentBytes, 0, firstPageData, 0, firstPageDataSize);

            DocumentPage firstPage = DocumentPage.CreateNew(_pageSize);
            int slotIndexResult = firstPage.AddDocument(firstPageData, pageIds, documentSize);

            byte[] firstPageBytes = firstPage.Serialize();
            _pageIO.WritePage(firstPageId, firstPageBytes);

            UpdateFSM(firstPageId, firstPage.GetFreeSpaceBytes());

            offset += firstPageDataSize;

            byte[] continuationBuffer = BufferPool.Rent(_pageSize);
            try
            {
                for (int i = 1; i < pagesNeeded; i++)
                {
                    int pageId = pageIds[i];
                    int remainingSize = documentSize - offset;
                    int chunkSize = Math.Min(remainingSize, _pageSize);

                    Array.Clear(continuationBuffer, 0, _pageSize);
                    Array.Copy(documentBytes, offset, continuationBuffer, 0, chunkSize);

                    _pageIO.WritePage(pageId, continuationBuffer);

                    UpdateFSM(pageId, _pageSize - chunkSize);

                    offset += chunkSize;
                }
            }
            finally
            {
                BufferPool.Return(continuationBuffer);
            }

            result = new DocumentLocation(firstPageId, slotIndexResult);
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
            DocumentPage page = DocumentPage.Deserialize(pageBuffer, _pageSize);

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
            DocumentPage page = DocumentPage.Deserialize(pageBuffer, _pageSize);

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

            page.Slots[slotIndex] = new SlotEntry
            {
                PageCount = 0,
                PageIds = null,
                TotalSize = 0,
                Offset = 0,
                Length = 0
            };

            byte[] serializedPage = page.Serialize();
            _pageIO.WritePage(pageId, serializedPage);

            UpdateFSM(pageId, page.GetFreeSpaceBytes());
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
}
