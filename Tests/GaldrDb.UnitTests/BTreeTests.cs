using System;
using System.IO;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class BTreeTests
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

    private BTree CreateBTree(string dbPath, int pageSize, int order, out IPageIO pageIO, out PageManager pageManager)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);

        pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        int rootPageId = pageManager.AllocatePage();

        BTreeNode rootNode = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        byte[] rootBytes = rootNode.Serialize();
        pageIO.WritePage(rootPageId, rootBytes);

        pageIO.Flush();

        BTree btree = new BTree(pageIO, pageManager, rootPageId, pageSize, order);
        BTree result = btree;

        return result;
    }

    [TestMethod]
    public void Insert_SingleItem_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        DocumentLocation location = new DocumentLocation(100, 5);
        btree.Insert(1, location);

        DocumentLocation found = btree.Search(1);

        bool result = found != null && found.PageId == 100 && found.SlotIndex == 5;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Insert_MultipleItems_CanSearchAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(5, new DocumentLocation(101, 1));
        btree.Insert(3, new DocumentLocation(102, 2));

        DocumentLocation found1 = btree.Search(1);
        DocumentLocation found3 = btree.Search(3);
        DocumentLocation found5 = btree.Search(5);

        bool result = found1.PageId == 100 && found1.SlotIndex == 0 &&
                      found3.PageId == 102 && found3.SlotIndex == 2 &&
                      found5.PageId == 101 && found5.SlotIndex == 1;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Search_NonExistingKey_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(5, new DocumentLocation(101, 1));

        DocumentLocation found = btree.Search(99);

        bool result = found == null;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Insert_ManyItems_CausesSplit()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        DocumentLocation found1 = btree.Search(1);
        DocumentLocation found10 = btree.Search(10);
        DocumentLocation found20 = btree.Search(20);

        bool result = found1 != null && found1.PageId == 101 && found1.SlotIndex == 1 &&
                      found10 != null && found10.PageId == 110 && found10.SlotIndex == 10 &&
                      found20 != null && found20.PageId == 120 && found20.SlotIndex == 20;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void GetRootPageId_AfterSplit_ReturnsNewRoot()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        int originalRoot = btree.GetRootPageId();

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        int newRoot = btree.GetRootPageId();

        bool result = newRoot != originalRoot;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Insert_SequentialKeys_MaintainsOrder()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation found = btree.Search(i);
            if (found == null || found.PageId != 100 + i || found.SlotIndex != i)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }

    [TestMethod]
    public void Insert_ReverseSequentialKeys_MaintainsOrder()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager);

        for (int i = 50; i >= 1; i--)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation found = btree.Search(i);
            if (found == null || found.PageId != 100 + i || found.SlotIndex != i)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }
}
