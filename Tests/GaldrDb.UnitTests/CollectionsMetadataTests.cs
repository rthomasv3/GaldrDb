using System;
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

        CollectionsMetadata metadata = new CollectionsMetadata(pageIO, 0, pageSize);
        CollectionsMetadata result = metadata;

        return result;
    }

    [TestMethod]
    public void AddCollection_NewCollection_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        CollectionsMetadata metadata = CreateCollectionsMetadata(dbPath, pageSize, out pageIO);

        CollectionEntry entry = metadata.AddCollection("users", 10);

        bool result = entry.Name == "users" && entry.RootPage == 10 && entry.DocumentCount == 0 && entry.NextId == 1;

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

        CollectionsMetadata metadata2 = new CollectionsMetadata(pageIO, 0, pageSize);
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
        entry.DocumentCount = 5;
        entry.NextId = 6;

        metadata.UpdateCollection(entry);

        CollectionEntry found = metadata.FindCollection("users");

        bool result = found.DocumentCount == 5 && found.NextId == 6;

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
}
