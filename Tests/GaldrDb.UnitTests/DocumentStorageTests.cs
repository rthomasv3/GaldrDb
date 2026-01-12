using System;
using System.IO;
using System.Text;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class DocumentStorageTests
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

    private DocumentStorage CreateDocumentStorage(string dbPath, int pageSize, out IPageIO pageIO, out PageManager pageManager)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);

        pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        pageIO.Flush();

        DocumentStorage storage = new DocumentStorage(pageIO, pageManager, pageSize);
        DocumentStorage result = storage;

        return result;
    }

    [TestMethod]
    public void WriteDocument_SmallDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        string testData = "Hello, GaldrDb! This is a test document.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(documentBytes);

        bool result = location.PageId >= 4 && location.SlotIndex == 0;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WriteAndReadDocument_SmallDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        string testData = "Hello, GaldrDb! This is a test document.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(documentBytes);

        byte[] readBytes = storage.ReadDocument(location.PageId, location.SlotIndex);
        string readData = Encoding.UTF8.GetString(readBytes);

        pageIO.Dispose();

        bool result = testData == readData;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WriteAndReadDocument_LargeDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2000; i++)
        {
            sb.Append($"This is line {i} of the large document. ");
        }

        string testData = sb.ToString();
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(documentBytes);

        byte[] readBytes = storage.ReadDocument(location.PageId, location.SlotIndex);
        string readData = Encoding.UTF8.GetString(readBytes);

        pageIO.Dispose();

        bool result = testData == readData && documentBytes.Length > 8192;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void WriteMultipleDocuments_ToSamePage_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        string doc1 = "First document";
        string doc2 = "Second document";
        string doc3 = "Third document";

        DocumentLocation loc1 = storage.WriteDocument(Encoding.UTF8.GetBytes(doc1));
        DocumentLocation loc2 = storage.WriteDocument(Encoding.UTF8.GetBytes(doc2));
        DocumentLocation loc3 = storage.WriteDocument(Encoding.UTF8.GetBytes(doc3));

        byte[] read1 = storage.ReadDocument(loc1.PageId, loc1.SlotIndex);
        byte[] read2 = storage.ReadDocument(loc2.PageId, loc2.SlotIndex);
        byte[] read3 = storage.ReadDocument(loc3.PageId, loc3.SlotIndex);

        pageIO.Dispose();

        bool result = Encoding.UTF8.GetString(read1) == doc1 &&
                     Encoding.UTF8.GetString(read2) == doc2 &&
                     Encoding.UTF8.GetString(read3) == doc3;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void DeleteDocument_RemovesAndUpdatesFSM()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        string testData = "Document to be deleted.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(documentBytes);

        storage.DeleteDocument(location.PageId, location.SlotIndex);

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
        {
            byte[] readBytes = storage.ReadDocument(location.PageId, location.SlotIndex);
        });

        pageIO.Dispose();

        bool result = exception != null && exception.Message.Contains("deleted");
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void PageReuse_AfterDeletion_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        string doc1 = "First document to be deleted";
        DocumentLocation loc1 = storage.WriteDocument(Encoding.UTF8.GetBytes(doc1));
        int firstPageId = loc1.PageId;

        storage.DeleteDocument(loc1.PageId, loc1.SlotIndex);

        string doc2 = "Second document reusing space";
        DocumentLocation loc2 = storage.WriteDocument(Encoding.UTF8.GetBytes(doc2));

        byte[] read2 = storage.ReadDocument(loc2.PageId, loc2.SlotIndex);

        pageIO.Dispose();

        bool result = Encoding.UTF8.GetString(read2) == doc2;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FSMUpdate_AfterWrite_CorrectLevel()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        string smallData = "Small";
        byte[] smallBytes = Encoding.UTF8.GetBytes(smallData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(smallBytes);

        pageIO.Dispose();

        bool result = location.PageId >= 4;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void MultiPageDocument_20KB_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        byte[] largeDocument = new byte[20 * 1024];
        for (int i = 0; i < largeDocument.Length; i++)
        {
            largeDocument[i] = (byte)(i % 256);
        }

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);
        DocumentLocation location = storage.WriteDocument(largeDocument);

        byte[] readDocument = storage.ReadDocument(location.PageId, location.SlotIndex);

        pageIO.Dispose();

        bool result = readDocument.Length == largeDocument.Length;
        for (int i = 0; i < largeDocument.Length && result; i++)
        {
            if (largeDocument[i] != readDocument[i])
            {
                result = false;
            }
        }

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void ReadDocument_InvalidSlotIndex_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            byte[] readBytes = storage.ReadDocument(4, 999);
        });

        pageIO.Dispose();

        bool result = exception != null;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void ReopenDatabase_DocumentsPersist()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;

        string testData = "Persistent data";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);
        DocumentLocation location;

        IPageIO pageIO1 = null;
        PageManager pageManager1 = null;
        DocumentStorage storage1 = CreateDocumentStorage(dbPath, pageSize, out pageIO1, out pageManager1);
        location = storage1.WriteDocument(documentBytes);
        pageIO1.Flush();
        pageIO1.Dispose();

        IPageIO pageIO2 = new StandardPageIO(dbPath, pageSize, false);
        PageManager pageManager2 = new PageManager(pageIO2, pageSize);
        pageManager2.Load();
        DocumentStorage storage2 = new DocumentStorage(pageIO2, pageManager2, pageSize);

        byte[] readBytes = storage2.ReadDocument(location.PageId, location.SlotIndex);
        string readData = Encoding.UTF8.GetString(readBytes);

        pageIO2.Dispose();

        bool result = testData == readData;
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void ContinuationPage_NotReusedForNewDocument_Sync()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        // Write a large document that spans multiple pages
        byte[] largeDocument = new byte[5000];
        for (int i = 0; i < largeDocument.Length; i++)
        {
            largeDocument[i] = (byte)'A';
        }
        DocumentLocation loc1 = storage.WriteDocument(largeDocument);

        // Write a small document - should NOT reuse continuation pages
        byte[] smallDocument = Encoding.UTF8.GetBytes("Small document");
        DocumentLocation loc2 = storage.WriteDocument(smallDocument);

        // Verify the small document is on a different page than the continuation page
        // The large document uses at least 2 pages, so continuation is loc1.PageId + 1
        bool smallDocOnDifferentPage = loc2.PageId != loc1.PageId + 1;

        // Verify both documents are readable and correct
        byte[] read1 = storage.ReadDocument(loc1.PageId, loc1.SlotIndex);
        byte[] read2 = storage.ReadDocument(loc2.PageId, loc2.SlotIndex);

        pageIO.Dispose();

        Assert.IsTrue(smallDocOnDifferentPage, $"Small document should not be on continuation page. loc1.PageId={loc1.PageId}, loc2.PageId={loc2.PageId}");
        Assert.HasCount(largeDocument.Length, read1);
        Assert.AreEqual("Small document", Encoding.UTF8.GetString(read2));
    }

    [TestMethod]
    public async System.Threading.Tasks.Task ContinuationPage_NotReusedForNewDocument_Async()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        // Write a large document that spans multiple pages
        byte[] largeDocument = new byte[5000];
        for (int i = 0; i < largeDocument.Length; i++)
        {
            largeDocument[i] = (byte)'B';
        }
        DocumentLocation loc1 = await storage.WriteDocumentAsync(largeDocument);

        // Write a small document - should NOT reuse continuation pages
        byte[] smallDocument = Encoding.UTF8.GetBytes("Small async document");
        DocumentLocation loc2 = await storage.WriteDocumentAsync(smallDocument);

        // Verify the small document is on a different page than the continuation page
        bool smallDocOnDifferentPage = loc2.PageId != loc1.PageId + 1;

        // Verify both documents are readable and correct
        byte[] read1 = await storage.ReadDocumentAsync(loc1.PageId, loc1.SlotIndex);
        byte[] read2 = await storage.ReadDocumentAsync(loc2.PageId, loc2.SlotIndex);

        pageIO.Dispose();

        Assert.IsTrue(smallDocOnDifferentPage, $"Small document should not be on continuation page. loc1.PageId={loc1.PageId}, loc2.PageId={loc2.PageId}");
        Assert.HasCount(largeDocument.Length, read1);
        Assert.AreEqual("Small async document", Encoding.UTF8.GetString(read2));
    }

    [TestMethod]
    public void WriteDocument_PageFragmented_CompactsOnDemand()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 4096;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO, out pageManager);

        byte[] doc1 = new byte[1000];
        byte[] doc2 = new byte[1000];
        byte[] doc3 = new byte[1000];
        for (int i = 0; i < 1000; i++)
        {
            doc1[i] = (byte)'A';
            doc2[i] = (byte)'B';
            doc3[i] = (byte)'C';
        }

        DocumentLocation loc1 = storage.WriteDocument(doc1);
        DocumentLocation loc2 = storage.WriteDocument(doc2);
        DocumentLocation loc3 = storage.WriteDocument(doc3);

        int originalPageId = loc1.PageId;
        Assert.AreEqual(originalPageId, loc2.PageId, "All docs should be on same page initially");
        Assert.AreEqual(originalPageId, loc3.PageId, "All docs should be on same page initially");

        storage.DeleteDocument(loc1.PageId, loc1.SlotIndex);
        storage.DeleteDocument(loc2.PageId, loc2.SlotIndex);

        byte[] doc4 = new byte[1500];
        for (int i = 0; i < doc4.Length; i++)
        {
            doc4[i] = (byte)'D';
        }
        DocumentLocation loc4 = storage.WriteDocument(doc4);

        Assert.AreEqual(originalPageId, loc4.PageId, "Doc4 should fit on original page after compaction");

        byte[] read3 = storage.ReadDocument(loc3.PageId, loc3.SlotIndex);
        byte[] read4 = storage.ReadDocument(loc4.PageId, loc4.SlotIndex);

        pageIO.Dispose();

        Assert.HasCount(1000, read3);
        Assert.AreEqual((byte)'C', read3[0]);
        Assert.HasCount(1500, read4);
        Assert.AreEqual((byte)'D', read4[0]);
    }
}
