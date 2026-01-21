using System;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class BitmapFsmGrowthTests
{
    private const int PAGE_SIZE = 4096;

    [TestMethod]
    public void AllocatePages_ExceedsFsmCapacity_GrowsSuccessfully()
    {
        // With 4KB page size:
        // - FSM uses 2 bits per page = 4 pages per byte
        // - One FSM page can track 4096 * 4 = 16,384 pages
        // - Initial setup uses 4 pages (header, bitmap, fsm, collections metadata)
        // - So we need to allocate more than 16,380 pages to trigger growth

        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int initialBitmapStart = pageManager.Header.BitmapStartPage;
        int initialFsmStart = pageManager.Header.FsmStartPage;
        int initialFsmCount = pageManager.Header.FsmPageCount;

        // Allocate pages until we exceed FSM capacity
        int fsmCapacity = PAGE_SIZE * 4; // 16,384 pages per FSM page
        int pagesToAllocate = fsmCapacity + 100; // Go past the limit

        int[] allocatedPages = new int[pagesToAllocate];
        for (int i = 0; i < pagesToAllocate; i++)
        {
            allocatedPages[i] = pageManager.AllocatePage();
        }

        // Verify growth occurred
        bool fsmGrew = pageManager.Header.FsmPageCount > initialFsmCount ||
                       pageManager.Header.FsmStartPage != initialFsmStart;

        Assert.IsTrue(fsmGrew, "FSM should have grown or relocated");
        Assert.IsGreaterThan(fsmCapacity, pageManager.Header.TotalPageCount, "Total page count should exceed original FSM capacity");

        // Verify all allocated pages are marked as allocated
        for (int i = 0; i < pagesToAllocate; i++)
        {
            bool isAllocated = pageManager.IsAllocated(allocatedPages[i]);
            Assert.IsTrue(isAllocated, $"Page {allocatedPages[i]} should be allocated");
        }

        // Verify we can still allocate more pages after growth
        int additionalPage = pageManager.AllocatePage();
        Assert.IsTrue(pageManager.IsAllocated(additionalPage), "Should be able to allocate pages after growth");

        pageIO.Dispose();
    }

    [TestMethod]
    public void AllocatePages_Growth_HeaderUpdatedCorrectly()
    {
        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int initialBitmapStart = pageManager.Header.BitmapStartPage;
        int initialFsmStart = pageManager.Header.FsmStartPage;
        int initialBitmapCount = pageManager.Header.BitmapPageCount;
        int initialFsmCount = pageManager.Header.FsmPageCount;

        // Allocate enough pages to trigger growth
        int fsmCapacity = PAGE_SIZE * 4;
        for (int i = 0; i < fsmCapacity + 50; i++)
        {
            pageManager.AllocatePage();
        }

        int newBitmapStart = pageManager.Header.BitmapStartPage;
        int newFsmStart = pageManager.Header.FsmStartPage;

        // Verify relocation occurred
        bool relocated = newBitmapStart != initialBitmapStart || newFsmStart != initialFsmStart;
        Assert.IsTrue(relocated, "Bitmap/FSM should have been relocated after exceeding capacity");

        // New locations should be beyond the initial 4 pages
        Assert.IsGreaterThan(3, newBitmapStart, "New bitmap should be beyond initial metadata pages");
        Assert.IsGreaterThan(3, newFsmStart, "New FSM should be beyond initial metadata pages");

        // FSM should be after bitmap in the new location
        Assert.IsGreaterThan(newBitmapStart, newFsmStart, "FSM should be after bitmap");

        // Page counts should have increased to handle larger file
        Assert.IsGreaterThanOrEqualTo(initialBitmapCount, pageManager.Header.BitmapPageCount, "Bitmap page count should not decrease");
        Assert.IsGreaterThanOrEqualTo(initialFsmCount, pageManager.Header.FsmPageCount, "FSM page count should not decrease");

        // Header should reflect current state - total pages exceeds original FSM capacity
        Assert.IsGreaterThan(fsmCapacity, pageManager.Header.TotalPageCount, "Total page count should exceed original FSM capacity");

        pageIO.Dispose();
    }

    [TestMethod]
    public void AllocatePages_Growth_OldPagesBecomesReusable()
    {
        // After growth relocates bitmap/FSM, the old pages should become available
        // for reuse as data pages. This test verifies that the old bitmap/FSM pages
        // get reallocated during subsequent allocations.

        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int oldBitmapStart = pageManager.Header.BitmapStartPage; // Page 1
        int oldFsmStart = pageManager.Header.FsmStartPage;       // Page 2

        // Allocate enough pages to trigger growth and then some more
        // to reuse the freed old bitmap/FSM pages
        int fsmCapacity = PAGE_SIZE * 4;
        int[] allocatedPages = new int[fsmCapacity + 50];
        for (int i = 0; i < fsmCapacity + 50; i++)
        {
            allocatedPages[i] = pageManager.AllocatePage();
        }

        int newBitmapStart = pageManager.Header.BitmapStartPage;
        int newFsmStart = pageManager.Header.FsmStartPage;

        // Verify relocation occurred
        Assert.AreNotEqual(oldBitmapStart, newBitmapStart, "Bitmap should have relocated");
        Assert.AreNotEqual(oldFsmStart, newFsmStart, "FSM should have relocated");

        // After growth, old bitmap/FSM pages are freed and should be reused
        // by subsequent allocations. Check that the old pages appear in our
        // allocated pages list (meaning they were reused as data pages).
        bool oldBitmapReused = Array.IndexOf(allocatedPages, oldBitmapStart) >= 0;
        bool oldFsmReused = Array.IndexOf(allocatedPages, oldFsmStart) >= 0;

        Assert.IsTrue(oldBitmapReused,
            $"Old bitmap page {oldBitmapStart} should have been reused as a data page");
        Assert.IsTrue(oldFsmReused,
            $"Old FSM page {oldFsmStart} should have been reused as a data page");

        pageIO.Dispose();
    }

    [TestMethod]
    public void AllocatePages_Growth_PersistsCorrectly()
    {
        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();
        pageManager.Flush();

        // Allocate enough pages to trigger growth
        int fsmCapacity = PAGE_SIZE * 4;
        int[] allocatedPages = new int[fsmCapacity + 50];
        for (int i = 0; i < fsmCapacity + 50; i++)
        {
            allocatedPages[i] = pageManager.AllocatePage();
        }

        pageManager.Flush();

        // Record state after growth
        int totalPagesAfterGrowth = pageManager.Header.TotalPageCount;
        int bitmapStartAfterGrowth = pageManager.Header.BitmapStartPage;
        int bitmapCountAfterGrowth = pageManager.Header.BitmapPageCount;
        int fsmStartAfterGrowth = pageManager.Header.FsmStartPage;
        int fsmCountAfterGrowth = pageManager.Header.FsmPageCount;

        // Simulate "reopen" by creating a new PageManager that loads from the same IO
        PageManager pageManager2 = new PageManager(pageIO, PAGE_SIZE);
        pageManager2.Load();

        // Verify header state is preserved
        Assert.AreEqual(totalPagesAfterGrowth, pageManager2.Header.TotalPageCount,
            "Total page count should be preserved");
        Assert.AreEqual(bitmapStartAfterGrowth, pageManager2.Header.BitmapStartPage,
            "Bitmap start page should be preserved");
        Assert.AreEqual(bitmapCountAfterGrowth, pageManager2.Header.BitmapPageCount,
            "Bitmap page count should be preserved");
        Assert.AreEqual(fsmStartAfterGrowth, pageManager2.Header.FsmStartPage,
            "FSM start page should be preserved");
        Assert.AreEqual(fsmCountAfterGrowth, pageManager2.Header.FsmPageCount,
            "FSM page count should be preserved");

        // Verify all originally allocated pages are still marked as allocated
        for (int i = 0; i < allocatedPages.Length; i++)
        {
            bool isAllocated = pageManager2.IsAllocated(allocatedPages[i]);
            Assert.IsTrue(isAllocated, $"Page {allocatedPages[i]} should still be allocated after reload");
        }

        // Verify we can still allocate pages
        int newPage = pageManager2.AllocatePage();
        Assert.IsTrue(pageManager2.IsAllocated(newPage), "Should be able to allocate after reload");

        pageIO.Dispose();
    }

    [TestMethod]
    public void AllocateBulkPages_ExceedsFsmCapacity_Success()
    {
        // Test bulk allocation (AllocatePages method) that triggers growth
        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int fsmCapacity = PAGE_SIZE * 4;
        int requestedCount = fsmCapacity + 100;

        // Request bulk allocation that exceeds capacity
        int[] pages = pageManager.AllocatePages(requestedCount);

        try
        {
            // IntArrayPool.Rent may return array larger than requested
            // Verify we got at least the requested count
            Assert.IsGreaterThanOrEqualTo(requestedCount, pages.Length, $"Should get at least {requestedCount} pages, got {pages.Length}");

            // Verify the first requestedCount pages are allocated
            for (int i = 0; i < requestedCount; i++)
            {
                Assert.IsTrue(pageManager.IsAllocated(pages[i]),
                    $"Page {pages[i]} at index {i} should be allocated");
            }

            // Verify growth occurred
            Assert.IsGreaterThan(fsmCapacity, pageManager.Header.TotalPageCount, "Total page count should exceed original capacity");
        }
        finally
        {
            IntArrayPool.Return(pages);
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void DeallocateAndReallocate_AfterGrowth_ReusesFreePages()
    {
        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int fsmCapacity = PAGE_SIZE * 4;

        // Allocate pages to trigger growth
        int[] allocatedPages = new int[fsmCapacity + 50];
        for (int i = 0; i < fsmCapacity + 50; i++)
        {
            allocatedPages[i] = pageManager.AllocatePage();
        }

        // Deallocate some pages
        int[] freedPages = new int[100];
        for (int i = 0; i < 100; i++)
        {
            int pageToFree = allocatedPages[i + 1000]; // Free pages in the middle
            pageManager.DeallocatePage(pageToFree);
            freedPages[i] = pageToFree;
        }

        // Verify deallocated pages are no longer allocated
        for (int i = 0; i < 100; i++)
        {
            Assert.IsFalse(pageManager.IsAllocated(freedPages[i]),
                $"Page {freedPages[i]} should be deallocated");
        }

        // Allocate new pages - should reuse freed pages
        int[] newPages = new int[100];
        for (int i = 0; i < 100; i++)
        {
            newPages[i] = pageManager.AllocatePage();
        }

        // At least some should be from the freed pool
        int reusedCount = 0;
        for (int i = 0; i < 100; i++)
        {
            if (Array.IndexOf(freedPages, newPages[i]) >= 0)
            {
                reusedCount++;
            }
        }

        Assert.IsGreaterThan(0, reusedCount, "Should reuse at least some freed pages");

        pageIO.Dispose();
    }

    [TestMethod]
    public void SparseInMemoryPageIO_MemoryEfficient()
    {
        // Verify that our sparse IO doesn't store empty pages
        InMemoryPageIO pageIO = new InMemoryPageIO(PAGE_SIZE);
        PageManager pageManager = new PageManager(pageIO, PAGE_SIZE);
        pageManager.Initialize();

        int fsmCapacity = PAGE_SIZE * 4;

        // Allocate many pages (which writes empty pages during expansion)
        for (int i = 0; i < fsmCapacity + 100; i++)
        {
            pageManager.AllocatePage();
        }

        // The sparse IO should only store non-empty pages (header, bitmap, fsm, collections metadata)
        // Not the thousands of empty data pages
        int storedPageCount = pageIO.PageCount;

        // Should be much less than total allocated pages
        // Expect: header (1) + bitmap (~2) + fsm (~2) + collections metadata (1) = ~6 pages
        Assert.IsLessThan(20, storedPageCount, $"Sparse IO should store few pages, but stored {storedPageCount}");

        pageIO.Dispose();
    }
}
