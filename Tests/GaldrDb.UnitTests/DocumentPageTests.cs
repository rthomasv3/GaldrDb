using System;
using GaldrDbEngine.Pages;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class DocumentPageTests
{
    private const int PAGE_SIZE = 8192;

    private void DeleteSlot(DocumentPage page, int slotIndex)
    {
        page.Slots[slotIndex] = new SlotEntry
        {
            PageCount = 0,
            PageIds = null,
            TotalSize = 0,
            Offset = 0,
            Length = 0
        };
    }

    [TestMethod]
    public void GetLogicalFreeSpace_NoDocuments_ReturnsFullFreeSpace()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        int logicalFree = page.GetLogicalFreeSpace();
        int contiguousFree = page.GetFreeSpaceBytes();

        Assert.AreEqual(contiguousFree, logicalFree);
    }

    [TestMethod]
    public void GetLogicalFreeSpace_NoHoles_EqualsContiguousFreeSpace()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[100];
        byte[] doc2 = new byte[200];
        page.AddDocument(doc1, new int[] { 1 }, 1, 100);
        page.AddDocument(doc2, new int[] { 1 }, 1, 200);

        int logicalFree = page.GetLogicalFreeSpace();
        int contiguousFree = page.GetFreeSpaceBytes();

        Assert.AreEqual(contiguousFree, logicalFree);
    }

    [TestMethod]
    public void GetLogicalFreeSpace_WithHoles_IncludesHoleSpace()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        int freeBeforeDelete = page.GetFreeSpaceBytes();

        DeleteSlot(page, 0);

        int contiguousFree = page.GetFreeSpaceBytes();
        int logicalFree = page.GetLogicalFreeSpace();

        Assert.AreEqual(freeBeforeDelete, contiguousFree, "Contiguous free space unchanged after delete");
        Assert.AreEqual(contiguousFree + 500, logicalFree, "Logical free space includes deleted doc's 500 bytes");
    }

    [TestMethod]
    public void NeedsCompaction_NoHoles_ReturnsFalse()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[100];
        page.AddDocument(doc1, new int[] { 1 }, 1, 100);

        Assert.IsFalse(page.NeedsCompaction());
    }

    [TestMethod]
    public void NeedsCompaction_WithSignificantHoles_ReturnsTrue()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);

        Assert.IsTrue(page.NeedsCompaction());
    }

    [TestMethod]
    public void NeedsCompaction_SmallHolesBelowThreshold_ReturnsFalse()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[50];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 50);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);

        Assert.IsFalse(page.NeedsCompaction(minimumGain: 64));
    }

    [TestMethod]
    public void NeedsCompaction_CustomThreshold_RespectsThreshold()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[50];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 50);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);

        Assert.IsFalse(page.NeedsCompaction(minimumGain: 100));
        Assert.IsTrue(page.NeedsCompaction(minimumGain: 40));
    }

    [TestMethod]
    public void Compact_SingleDeletedDocument_ReclaimsSpace()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        int freeBeforeDelete = page.GetFreeSpaceBytes();
        DeleteSlot(page, 0);
        int freeAfterDelete = page.GetFreeSpaceBytes();

        page.Compact();

        int freeAfterCompact = page.GetFreeSpaceBytes();

        Assert.AreEqual(freeBeforeDelete, freeAfterDelete, "Delete doesn't change contiguous free space");
        Assert.AreEqual(freeBeforeDelete + 500, freeAfterCompact, "Compact reclaims deleted doc's space");
    }

    [TestMethod]
    public void Compact_MultipleDeletedDocuments_ReclaimsAllHoles()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[200];
        byte[] doc2 = new byte[300];
        byte[] doc3 = new byte[400];
        byte[] doc4 = new byte[100];
        page.AddDocument(doc1, new int[] { 1 }, 1, 200);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);
        page.AddDocument(doc3, new int[] { 1 }, 1, 400);
        page.AddDocument(doc4, new int[] { 1 }, 1, 100);

        int freeBeforeDeletes = page.GetFreeSpaceBytes();

        DeleteSlot(page, 0);
        DeleteSlot(page, 2);

        page.Compact();

        int freeAfterCompact = page.GetFreeSpaceBytes();
        int expectedReclaimed = 200 + 400;

        Assert.AreEqual(freeBeforeDeletes + expectedReclaimed, freeAfterCompact);
    }

    [TestMethod]
    public void Compact_NoDeletedDocuments_NoChange()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[200];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 200);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        int freeBefore = page.GetFreeSpaceBytes();
        ushort freeSpaceEndBefore = page.FreeSpaceEnd;

        page.Compact();

        Assert.AreEqual(freeBefore, page.GetFreeSpaceBytes());
        Assert.AreEqual(freeSpaceEndBefore, page.FreeSpaceEnd);
    }

    [TestMethod]
    public void Compact_PreservesSlotIndices()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[100];
        byte[] doc2 = new byte[200];
        byte[] doc3 = new byte[300];

        for (int i = 0; i < 100; i++) doc1[i] = 1;
        for (int i = 0; i < 200; i++) doc2[i] = 2;
        for (int i = 0; i < 300; i++) doc3[i] = 3;

        page.AddDocument(doc1, new int[] { 1 }, 1, 100);
        page.AddDocument(doc2, new int[] { 1 }, 1, 200);
        page.AddDocument(doc3, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 1);

        page.Compact();

        byte[] retrieved0 = page.GetDocumentData(0);
        byte[] retrieved2 = page.GetDocumentData(2);

        Assert.HasCount(100, retrieved0);
        Assert.HasCount(300, retrieved2);
        Assert.AreEqual((byte)1, retrieved0[0]);
        Assert.AreEqual((byte)3, retrieved2[0]);
    }

    [TestMethod]
    public void Compact_DataIntegrity_DocumentsStillReadable()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[256];
        byte[] doc2 = new byte[512];
        byte[] doc3 = new byte[128];

        for (int i = 0; i < doc1.Length; i++) doc1[i] = (byte)(i % 256);
        for (int i = 0; i < doc2.Length; i++) doc2[i] = (byte)((i * 2) % 256);
        for (int i = 0; i < doc3.Length; i++) doc3[i] = (byte)((i * 3) % 256);

        page.AddDocument(doc1, new int[] { 1 }, 1, 256);
        page.AddDocument(doc2, new int[] { 1 }, 1, 512);
        page.AddDocument(doc3, new int[] { 1 }, 1, 128);

        DeleteSlot(page, 1);

        page.Compact();

        byte[] retrieved0 = page.GetDocumentData(0);
        byte[] retrieved2 = page.GetDocumentData(2);

        CollectionAssert.AreEqual(doc1, retrieved0, "Doc1 data intact after compaction");
        CollectionAssert.AreEqual(doc3, retrieved2, "Doc3 data intact after compaction");
    }

    [TestMethod]
    public void Compact_EmptyPage_HandlesGracefully()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        int freeBefore = page.GetFreeSpaceBytes();

        page.Compact();

        Assert.AreEqual(freeBefore, page.GetFreeSpaceBytes());
    }

    [TestMethod]
    public void Compact_AllSlotsDeleted_EntireDataRegionFree()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);
        DeleteSlot(page, 1);

        page.Compact();

        Assert.AreEqual((ushort)PAGE_SIZE, page.FreeSpaceEnd, "FreeSpaceEnd should be at page end");
    }

    [TestMethod]
    public void Compact_FreeSpaceOffsetUnchanged()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        ushort freeSpaceOffsetBefore = page.FreeSpaceOffset;

        DeleteSlot(page, 0);
        page.Compact();

        Assert.AreEqual(freeSpaceOffsetBefore, page.FreeSpaceOffset, "Slot array size unchanged");
    }

    [TestMethod]
    public void Compact_AfterCompaction_NeedsCompactionReturnsFalse()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);

        Assert.IsTrue(page.NeedsCompaction(), "Should need compaction before");

        page.Compact();

        Assert.IsFalse(page.NeedsCompaction(), "Should not need compaction after");
    }

    [TestMethod]
    public void Compact_LogicalFreeEqualsContiguousAfterCompaction()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        byte[] doc3 = new byte[200];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);
        page.AddDocument(doc3, new int[] { 1 }, 1, 200);

        DeleteSlot(page, 0);
        DeleteSlot(page, 2);

        Assert.AreNotEqual(page.GetLogicalFreeSpace(), page.GetFreeSpaceBytes(), "Should differ before compact");

        page.Compact();

        Assert.AreEqual(page.GetLogicalFreeSpace(), page.GetFreeSpaceBytes(), "Should be equal after compact");
    }

    [TestMethod]
    public void Compact_MultipleCompactions_Idempotent()
    {
        DocumentPage page = DocumentPage.CreateNew(PAGE_SIZE);

        byte[] doc1 = new byte[500];
        byte[] doc2 = new byte[300];
        page.AddDocument(doc1, new int[] { 1 }, 1, 500);
        page.AddDocument(doc2, new int[] { 1 }, 1, 300);

        DeleteSlot(page, 0);

        page.Compact();
        int freeAfterFirst = page.GetFreeSpaceBytes();
        ushort freeSpaceEndAfterFirst = page.FreeSpaceEnd;

        page.Compact();

        Assert.AreEqual(freeAfterFirst, page.GetFreeSpaceBytes());
        Assert.AreEqual(freeSpaceEndAfterFirst, page.FreeSpaceEnd);
    }
}
