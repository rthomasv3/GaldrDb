using System;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;

namespace GaldrDbCore.Storage;

public class DocumentStorage
{
    private readonly IPageIO _pageIO;
    private readonly Bitmap _bitmap;
    private readonly FreeSpaceMap _fsm;
    private readonly int _pageSize;

    public DocumentStorage(IPageIO pageIO, Bitmap bitmap, FreeSpaceMap fsm, int pageSize)
    {
        _pageIO = pageIO;
        _bitmap = bitmap;
        _fsm = fsm;
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

        if (pagesNeeded == 1)
        {
            int pageId = FindOrAllocatePageForDocument(documentSize);
            pageIds[0] = pageId;

            byte[] pageBytes = _pageIO.ReadPage(pageId);
            DocumentPage page = null;

            if (IsPageInitialized(pageBytes))
            {
                page = DocumentPage.Deserialize(pageBytes, _pageSize);
            }
            else
            {
                page = DocumentPage.CreateNew(_pageSize);
            }

            int slotIndex = page.AddDocument(documentBytes, pageIds, documentSize);

            byte[] serializedPage = page.Serialize();
            _pageIO.WritePage(pageId, serializedPage);

            UpdateFSM(pageId, page.GetFreeSpaceBytes());

            DocumentLocation result = new DocumentLocation(pageId, slotIndex);

            return result;
        }
        else
        {
            for (int i = 0; i < pagesNeeded; i++)
            {
                int pageId = _bitmap.FindFreePage();

                if (pageId == -1)
                {
                    throw new InvalidOperationException("No free pages available");
                }

                _bitmap.AllocatePage(pageId);
                pageIds[i] = pageId;
            }

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

            for (int i = 1; i < pagesNeeded; i++)
            {
                int pageId = pageIds[i];
                int remainingSize = documentSize - offset;
                int chunkSize = Math.Min(remainingSize, _pageSize);

                byte[] continuationPage = new byte[_pageSize];
                Array.Copy(documentBytes, offset, continuationPage, 0, chunkSize);

                _pageIO.WritePage(pageId, continuationPage);

                UpdateFSM(pageId, _pageSize - chunkSize);

                offset += chunkSize;
            }

            _bitmap.WriteToDisk();
            _fsm.WriteToDisk();

            DocumentLocation finalResult = new DocumentLocation(firstPageId, slotIndexResult);

            return finalResult;
        }
    }

    public byte[] ReadDocument(int pageId, int slotIndex)
    {
        byte[] pageBytes = _pageIO.ReadPage(pageId);
        DocumentPage page = DocumentPage.Deserialize(pageBytes, _pageSize);

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
            byte[] documentData = page.GetDocumentData(slotIndex);
            byte[] result = documentData;

            return result;
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
                byte[] continuationPageBytes = _pageIO.ReadPage(continuationPageId);
                int remainingSize = entry.TotalSize - offset;
                int chunkSize = Math.Min(remainingSize, _pageSize);

                Array.Copy(continuationPageBytes, 0, fullDocument, offset, chunkSize);
                offset += chunkSize;
            }

            byte[] finalResult = fullDocument;

            return finalResult;
        }
    }

    public void DeleteDocument(int pageId, int slotIndex)
    {
        byte[] pageBytes = _pageIO.ReadPage(pageId);
        DocumentPage page = DocumentPage.Deserialize(pageBytes, _pageSize);

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
                _bitmap.DeallocatePage(continuationPageId);
                _fsm.SetFreeSpaceLevel(continuationPageId, FreeSpaceLevel.High);
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

        _bitmap.WriteToDisk();
        _fsm.WriteToDisk();
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

        int pageId = _fsm.FindPageWithSpace(minLevel);

        if (pageId == -1 || !_bitmap.IsAllocated(pageId) || pageId < 4)
        {
            pageId = _bitmap.FindFreePage();

            if (pageId == -1)
            {
                throw new InvalidOperationException("No free pages available");
            }

            _bitmap.AllocatePage(pageId);
            _bitmap.WriteToDisk();
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

        _fsm.SetFreeSpaceLevel(pageId, level);
        _fsm.WriteToDisk();
    }

    private bool IsPageInitialized(byte[] pageBytes)
    {
        bool result = pageBytes[0] == PageConstants.PAGE_TYPE_DOCUMENT;

        return result;
    }
}
