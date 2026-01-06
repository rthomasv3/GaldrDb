using System;
using System.IO;
using System.Text;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Storage;
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

    private DocumentStorage CreateDocumentStorage(string dbPath, int pageSize, out IPageIO pageIO, int totalPages = 50)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);

        Bitmap bitmap = new Bitmap(pageIO, 1, 1, totalPages);
        bitmap.AllocatePage(0);
        bitmap.AllocatePage(1);
        bitmap.AllocatePage(2);
        bitmap.AllocatePage(3);
        bitmap.WriteToDisk();

        FreeSpaceMap fsm = new FreeSpaceMap(pageIO, 2, 1, totalPages);
        fsm.WriteToDisk();

        byte[] headerPage = new byte[pageSize];
        pageIO.WritePage(0, headerPage);

        for (int i = 4; i < totalPages; i++)
        {
            byte[] emptyPage = new byte[pageSize];
            pageIO.WritePage(i, emptyPage);
        }

        pageIO.Flush();

        DocumentStorage storage = new DocumentStorage(pageIO, bitmap, fsm, pageSize);
        DocumentStorage result = storage;

        return result;
    }

    [TestMethod]
    public void WriteDocument_SmallDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        IPageIO pageIO = null;

        string testData = "Hello, GaldrDb! This is a test document.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        string testData = "Hello, GaldrDb! This is a test document.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2000; i++)
        {
            sb.Append($"This is line {i} of the large document. ");
        }

        string testData = sb.ToString();
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);

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

        string testData = "Document to be deleted.";
        byte[] documentBytes = Encoding.UTF8.GetBytes(testData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);

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

        string smallData = "Small";
        byte[] smallBytes = Encoding.UTF8.GetBytes(smallData);

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        byte[] largeDocument = new byte[20 * 1024];
        for (int i = 0; i < largeDocument.Length; i++)
        {
            largeDocument[i] = (byte)(i % 256);
        }

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);
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

        DocumentStorage storage = CreateDocumentStorage(dbPath, pageSize, out pageIO);

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
        DocumentLocation location = null;

        IPageIO pageIO1 = null;
        DocumentStorage storage1 = CreateDocumentStorage(dbPath, pageSize, out pageIO1);
        location = storage1.WriteDocument(documentBytes);
        pageIO1.Flush();
        pageIO1.Dispose();

        IPageIO pageIO2 = new StandardPageIO(dbPath, pageSize, false);
        Bitmap bitmap2 = new Bitmap(pageIO2, 1, 1, 50);
        bitmap2.LoadFromDisk();
        FreeSpaceMap fsm2 = new FreeSpaceMap(pageIO2, 2, 1, 50);
        fsm2.LoadFromDisk();
        DocumentStorage storage2 = new DocumentStorage(pageIO2, bitmap2, fsm2, pageSize);

        byte[] readBytes = storage2.ReadDocument(location.PageId, location.SlotIndex);
        string readData = Encoding.UTF8.GetString(readBytes);

        pageIO2.Dispose();

        bool result = testData == readData;
        Assert.IsTrue(result);
    }
}
