using System;
using System.Collections.Generic;
using System.IO;
using GaldrDbEngine.IO;
using GaldrDbEngine.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class CollectionsMetadataTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbTests_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    private CollectionsMetadata CreateCollectionsMetadata(string dbPath, int pageSize, out IPageIO pageIO)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);

        byte[] emptyPage = new byte[pageSize];
        pageIO.WritePage(0, emptyPage);
        pageIO.Flush();

        return new CollectionsMetadata(pageIO, 0, 1, pageSize);
    }

    [TestMethod]
    public void AddCollection_NewCollection_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        CollectionEntry entry = metadata.AddCollection("users", 10);

        bool result = entry.Name == "users" && entry.RootPage == 10 && entry.NextId == 1;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FindCollection_ExistingCollection_ReturnsCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);
        CollectionEntry found = metadata.FindCollection("users");

        bool result = found.Name == "users" && found.RootPage == 10;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FindCollection_NonExistingCollection_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        CollectionEntry found = metadata.FindCollection("nonexistent");

        bool result = found == null;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WriteToDisk_AndLoadFromDisk_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);
        metadata.AddCollection("products", 20);
        metadata.WriteToDisk();

        CollectionsMetadata metadata2 = new CollectionsMetadata(pageIO, 0, 1, pageSize);
        metadata2.LoadFromDisk();

        CollectionEntry users = metadata2.FindCollection("users");
        CollectionEntry products = metadata2.FindCollection("products");

        bool result = users.Name == "users" && users.RootPage == 10 &&
                      products.Name == "products" && products.RootPage == 20;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void UpdateCollection_ExistingCollection_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        CollectionEntry entry = metadata.AddCollection("users", 10);
        entry.RootPage = 42;

        metadata.UpdateCollection(entry);

        CollectionEntry found = metadata.FindCollection("users");

        bool result = found.RootPage == 42;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void RemoveCollection_ExistingCollection_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);
        metadata.RemoveCollection("users");

        CollectionEntry found = metadata.FindCollection("users");

        bool result = found == null;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void GetCollectionCount_MultipleCollections_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);
        metadata.AddCollection("products", 20);
        metadata.AddCollection("orders", 30);

        int count = metadata.GetCollectionCount();

        pageIO.Dispose();

        Assert.AreEqual(3, count);
    }

    [TestMethod]
    public void CalculateSerializedSize_EmptyMetadata_Returns4Bytes()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        int size = metadata.CalculateSerializedSize();

        pageIO.Dispose();

        Assert.AreEqual(4, size);
    }

    [TestMethod]
    public void CalculateSerializedSize_WithCollections_ReturnsCorrectSize()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);

        int size = metadata.CalculateSerializedSize();

        pageIO.Dispose();

        // 4 (count) + 1 (version) + 4 (name length) + 5 (name "users") + 4 (RootPage) + 4 (index count) = 22
        Assert.AreEqual(22, size);
    }

    [TestMethod]
    public void GetPagesNeeded_SmallData_Returns1()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        metadata.AddCollection("users", 10);

        int pagesNeeded = metadata.GetPagesNeeded();

        pageIO.Dispose();

        Assert.AreEqual(1, pagesNeeded);
    }

    [TestMethod]
    public void GetPagesNeeded_LargeData_ReturnsMultiplePages()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 512;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        // Each collection with 10-char name: 1 + 4 + 10 + 4 + 4 = 23 bytes
        // With 512-byte page and 4-byte header: (512 - 4) / 23 ≈ 22 collections per page
        // Add 40 collections to need at least 2 pages
        for (int i = 0; i < 40; i++)
        {
            metadata.AddCollection($"collect_{i:D3}", i);
        }

        int pagesNeeded = metadata.GetPagesNeeded();

        pageIO.Dispose();

        Assert.IsGreaterThanOrEqualTo(2, pagesNeeded, $"Expected at least 2 pages, got {pagesNeeded}");
    }

    [TestMethod]
    public void WriteToDisk_InsufficientPages_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 512;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        byte[] emptyPage = new byte[pageSize];
        pageIO.WritePage(0, emptyPage);
        pageIO.Flush();

        // Create metadata with only 1 page
        CollectionsMetadata metadata = new CollectionsMetadata(pageIO, 0, 1, pageSize);

        // Add enough collections to exceed 1 page
        for (int i = 0; i < 40; i++)
        {
            metadata.AddCollection($"collect_{i:D3}", i);
        }

        bool threwException = false;
        try
        {
            metadata.WriteToDisk();
        }
        catch (InvalidOperationException)
        {
            threwException = true;
        }

        pageIO.Dispose();

        Assert.IsTrue(threwException, "Expected InvalidOperationException when writing exceeds allocated pages");
    }

    [TestMethod]
    public void MultiPage_WriteToDisk_AndLoadFromDisk_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 512;
        int pageCount = 3;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        // Pre-allocate multiple pages
        byte[] emptyPage = new byte[pageSize];
        for (int i = 0; i < pageCount; i++)
        {
            pageIO.WritePage(i, emptyPage);
        }
        pageIO.Flush();

        CollectionsMetadata metadata = new CollectionsMetadata(pageIO, 0, pageCount, pageSize);

        // Add enough collections to span multiple pages
        int collectionCount = 40;
        for (int i = 0; i < collectionCount; i++)
        {
            metadata.AddCollection($"collect_{i:D3}", i * 10);
        }

        metadata.WriteToDisk();

        // Create new metadata instance and load from disk
        CollectionsMetadata metadata2 = new CollectionsMetadata(pageIO, 0, pageCount, pageSize);
        metadata2.LoadFromDisk();

        // Verify all collections were loaded correctly
        bool allFound = true;
        for (int i = 0; i < collectionCount; i++)
        {
            CollectionEntry entry = metadata2.FindCollection($"collect_{i:D3}");
            if (entry == null || entry.RootPage != i * 10)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound, "Not all collections were correctly persisted and loaded across multiple pages");
        Assert.AreEqual(collectionCount, metadata2.GetCollectionCount());
    }

    [TestMethod]
    public void MultiPage_DataSpansPageBoundary_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 256;
        int pageCount = 8;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        // Pre-allocate multiple pages
        byte[] emptyPage = new byte[pageSize];
        for (int i = 0; i < pageCount; i++)
        {
            pageIO.WritePage(i, emptyPage);
        }
        pageIO.Flush();

        CollectionsMetadata metadata = new CollectionsMetadata(pageIO, 0, pageCount, pageSize);

        // Add collections with longer names to ensure data spans page boundaries
        int collectionCount = 30;
        for (int i = 0; i < collectionCount; i++)
        {
            metadata.AddCollection($"my_long_collection_name_{i:D4}", i * 100);
        }

        metadata.WriteToDisk();

        // Verify pages needed exceeds 1 (ensures we're actually testing multi-page)
        int pagesNeeded = metadata.GetPagesNeeded();
        Assert.IsGreaterThan(1, pagesNeeded, $"Test should require multiple pages, but only needs {pagesNeeded}");

        // Load and verify
        CollectionsMetadata metadata2 = new CollectionsMetadata(pageIO, 0, pageCount, pageSize);
        metadata2.LoadFromDisk();

        bool allCorrect = true;
        for (int i = 0; i < collectionCount; i++)
        {
            CollectionEntry entry = metadata2.FindCollection($"my_long_collection_name_{i:D4}");
            if (entry == null || entry.RootPage != i * 100)
            {
                allCorrect = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allCorrect, "Collections spanning page boundaries were not correctly handled");
    }

    #region Integration Tests - PageManager + CollectionsMetadata Growth

    [TestMethod]
    public void Integration_GrowCollectionsMetadata_ExpandsInPlace_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 512;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        // Initialize PageManager (creates header, bitmap, fsm, collections metadata)
        PageManager pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        // Create CollectionsMetadata with initial allocation from header
        CollectionsMetadata metadata = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);

        // Verify initial state
        Assert.AreEqual(1, pageManager.Header.CollectionsMetadataPageCount);
        int initialStartPage = pageManager.Header.CollectionsMetadataStartPage;

        // Add collections until we need more pages
        int collectionsToAdd = 30;
        for (int i = 0; i < collectionsToAdd; i++)
        {
            metadata.AddCollection($"collection_{i:D3}", i * 10);
        }

        // Check if growth is needed
        int pagesNeeded = metadata.GetPagesNeeded();
        Assert.IsGreaterThan(1, pagesNeeded, $"Test should require growth, but only needs {pagesNeeded} pages");

        // Trigger growth
        int additional = pagesNeeded - metadata.GetCurrentPageCount();
        pageManager.GrowCollectionsMetadata(metadata, additional);

        // Write to disk
        metadata.WriteToDisk();

        // Verify header was updated
        Assert.AreEqual(pagesNeeded, pageManager.Header.CollectionsMetadataPageCount);

        // Reload and verify all collections survived
        CollectionsMetadata metadata2 = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);
        metadata2.LoadFromDisk();

        Assert.AreEqual(collectionsToAdd, metadata2.GetCollectionCount());

        for (int i = 0; i < collectionsToAdd; i++)
        {
            CollectionEntry entry = metadata2.FindCollection($"collection_{i:D3}");
            Assert.IsNotNull(entry, $"Collection collection_{i:D3} not found after growth");
            Assert.AreEqual(i * 10, entry.RootPage);
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void Integration_GrowCollectionsMetadata_RelocatesWhenNeeded_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 512;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        // Initialize PageManager
        PageManager pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        // Allocate pages immediately after collections metadata to force relocation
        int collectionsMetadataPage = pageManager.Header.CollectionsMetadataStartPage;
        List<int> allocatedPages = new List<int>();
        for (int i = 0; i < 5; i++)
        {
            int pageId = pageManager.AllocatePage();
            allocatedPages.Add(pageId);
        }

        // Create CollectionsMetadata
        CollectionsMetadata metadata = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);

        // Add collections to trigger growth
        int collectionsToAdd = 30;
        for (int i = 0; i < collectionsToAdd; i++)
        {
            metadata.AddCollection($"collection_{i:D3}", i * 10);
        }

        int pagesNeeded = metadata.GetPagesNeeded();
        Assert.IsGreaterThan(1, pagesNeeded, $"Test should require growth, but only needs {pagesNeeded} pages");

        int originalStartPage = pageManager.Header.CollectionsMetadataStartPage;

        // Trigger growth - should relocate since adjacent pages are taken
        int additional = pagesNeeded - metadata.GetCurrentPageCount();
        pageManager.GrowCollectionsMetadata(metadata, additional);

        // Verify start page changed (relocation occurred)
        Assert.AreNotEqual(originalStartPage, pageManager.Header.CollectionsMetadataStartPage,
            "Expected relocation but start page didn't change");

        // Write and reload
        metadata.WriteToDisk();

        CollectionsMetadata metadata2 = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);
        metadata2.LoadFromDisk();

        // Verify all data survived relocation
        Assert.AreEqual(collectionsToAdd, metadata2.GetCollectionCount());

        for (int i = 0; i < collectionsToAdd; i++)
        {
            CollectionEntry entry = metadata2.FindCollection($"collection_{i:D3}");
            Assert.IsNotNull(entry, $"Collection collection_{i:D3} not found after relocation");
            Assert.AreEqual(i * 10, entry.RootPage);
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void Integration_GrowCollectionsMetadata_MultipleGrowths_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 256;
        IPageIO pageIO = new StandardPageIO(dbPath, pageSize, true);

        // Initialize PageManager
        PageManager pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        CollectionsMetadata metadata = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);

        // Add collections in batches, growing as needed
        int totalCollections = 0;
        for (int batch = 0; batch < 3; batch++)
        {
            // Add 20 collections per batch
            for (int i = 0; i < 20; i++)
            {
                metadata.AddCollection($"batch{batch}_coll_{i:D3}", totalCollections * 10);
                totalCollections++;
            }

            // Grow if needed and write
            int pagesNeeded = metadata.GetPagesNeeded();
            if (pagesNeeded > metadata.GetCurrentPageCount())
            {
                int additional = pagesNeeded - metadata.GetCurrentPageCount();
                pageManager.GrowCollectionsMetadata(metadata, additional);
            }
            metadata.WriteToDisk();
        }

        // Verify final state
        Assert.IsGreaterThan(1, pageManager.Header.CollectionsMetadataPageCount, "Expected multiple pages after all growths");

        // Reload and verify
        CollectionsMetadata metadata2 = new CollectionsMetadata(
            pageIO,
            pageManager.Header.CollectionsMetadataStartPage,
            pageManager.Header.CollectionsMetadataPageCount,
            pageSize);
        metadata2.LoadFromDisk();

        Assert.AreEqual(totalCollections, metadata2.GetCollectionCount());

        // Spot check some collections from each batch
        Assert.IsNotNull(metadata2.FindCollection("batch0_coll_005"));
        Assert.IsNotNull(metadata2.FindCollection("batch1_coll_010"));
        Assert.IsNotNull(metadata2.FindCollection("batch2_coll_015"));

        pageIO.Dispose();
    }

    #endregion
}
