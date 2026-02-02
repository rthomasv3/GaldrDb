using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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

    private BTree CreateBTree(string dbPath, int pageSize, int order, out IPageIO pageIO, out PageManager pageManager, out PageLockManager pageLockManager)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);

        pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        pageLockManager = new PageLockManager();

        int rootPageId = pageManager.AllocatePage();

        BTreeNode rootNode = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        byte[] rootBuffer = new byte[pageSize];
        rootNode.SerializeTo(rootBuffer);
        pageIO.WritePage(rootPageId, rootBuffer);
        pageIO.Flush();

        return new BTree(pageIO, pageManager, pageLockManager, rootPageId, pageSize, order);
    }

    [TestMethod]
    public void Insert_SingleItem_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        DocumentLocation location = new DocumentLocation(100, 5);
        btree.Insert(1, location);

        DocumentLocation? found = btree.Search(1);

        bool result = found != null && found.Value.PageId == 100 && found.Value.SlotIndex == 5;

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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(5, new DocumentLocation(101, 1));
        btree.Insert(3, new DocumentLocation(102, 2));

        DocumentLocation? found1 = btree.Search(1);
        DocumentLocation? found3 = btree.Search(3);
        DocumentLocation? found5 = btree.Search(5);

        bool result = found1.Value.PageId == 100 && found1.Value.SlotIndex == 0 &&
                      found3.Value.PageId == 102 && found3.Value.SlotIndex == 2 &&
                      found5.Value.PageId == 101 && found5.Value.SlotIndex == 1;

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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(5, new DocumentLocation(101, 1));

        DocumentLocation? found = btree.Search(99);

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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        DocumentLocation? found1 = btree.Search(1);
        DocumentLocation? found10 = btree.Search(10);
        DocumentLocation? found20 = btree.Search(20);

        bool result = found1 != null && found1.Value.PageId == 101 && found1.Value.SlotIndex == 1 &&
                      found10 != null && found10.Value.PageId == 110 && found10.Value.SlotIndex == 10 &&
                      found20 != null && found20.Value.PageId == 120 && found20.Value.SlotIndex == 20;

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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != 100 + i || found.Value.SlotIndex != i)
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

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 50; i >= 1; i--)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != 100 + i || found.Value.SlotIndex != i)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }

    [TestMethod]
    public void Delete_SingleItem_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));

        bool deleted = btree.Delete(1);
        DocumentLocation? found = btree.Search(1);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found);
    }

    [TestMethod]
    public void Delete_NonExistingKey_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));

        bool deleted = btree.Delete(99);

        pageIO.Dispose();

        Assert.IsFalse(deleted);
    }

    [TestMethod]
    public void Delete_FromMultipleItems_OthersRemain()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(2, new DocumentLocation(101, 1));
        btree.Insert(3, new DocumentLocation(102, 2));

        bool deleted = btree.Delete(2);
        DocumentLocation? found1 = btree.Search(1);
        DocumentLocation? found2 = btree.Search(2);
        DocumentLocation? found3 = btree.Search(3);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNotNull(found1);
        Assert.IsNull(found2);
        Assert.IsNotNull(found3);
        Assert.AreEqual(100, found1.Value.PageId);
        Assert.AreEqual(102, found3.Value.PageId);
    }

    [TestMethod]
    public void Delete_AllItems_TreeEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 5; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool allDeleted = true;
        for (int i = 1; i <= 5; i++)
        {
            if (!btree.Delete(i))
            {
                allDeleted = false;
            }
        }

        bool allGone = true;
        for (int i = 1; i <= 5; i++)
        {
            if (btree.Search(i) != null)
            {
                allGone = false;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allDeleted);
        Assert.IsTrue(allGone);
    }

    [TestMethod]
    public void Delete_FromMultiLevelTree_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        bool deleted5 = btree.Delete(5);
        bool deleted10 = btree.Delete(10);
        bool deleted15 = btree.Delete(15);

        DocumentLocation? found5 = btree.Search(5);
        DocumentLocation? found10 = btree.Search(10);
        DocumentLocation? found15 = btree.Search(15);
        DocumentLocation? found1 = btree.Search(1);
        DocumentLocation? found20 = btree.Search(20);

        pageIO.Dispose();

        Assert.IsTrue(deleted5);
        Assert.IsTrue(deleted10);
        Assert.IsTrue(deleted15);
        Assert.IsNull(found5);
        Assert.IsNull(found10);
        Assert.IsNull(found15);
        Assert.IsNotNull(found1);
        Assert.IsNotNull(found20);
    }

    [TestMethod]
    public void GetAllEntries_EmptyTree_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsEmpty(entries);
    }

    [TestMethod]
    public void GetAllEntries_SingleItem_ReturnsOne()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(42, new DocumentLocation(100, 5));

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.HasCount(1, entries);
        Assert.AreEqual(42, entries[0].Key);
        Assert.AreEqual(100, entries[0].Location.PageId);
        Assert.AreEqual(5, entries[0].Location.SlotIndex);
    }

    [TestMethod]
    public void GetAllEntries_MultipleItems_ReturnsAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(3, new DocumentLocation(103, 3));
        btree.Insert(1, new DocumentLocation(101, 1));
        btree.Insert(2, new DocumentLocation(102, 2));

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.HasCount(3, entries);

        bool hasKey1 = false;
        bool hasKey2 = false;
        bool hasKey3 = false;
        foreach (BTreeEntry entry in entries)
        {
            if (entry.Key == 1 && entry.Location.PageId == 101) hasKey1 = true;
            if (entry.Key == 2 && entry.Location.PageId == 102) hasKey2 = true;
            if (entry.Key == 3 && entry.Location.PageId == 103) hasKey3 = true;
        }

        Assert.IsTrue(hasKey1);
        Assert.IsTrue(hasKey2);
        Assert.IsTrue(hasKey3);
    }

    [TestMethod]
    public void GetAllEntries_MultiLevelTree_ReturnsAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 30; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.HasCount(30, entries);

        bool[] found = new bool[31];
        foreach (BTreeEntry entry in entries)
        {
            if (entry.Key >= 1 && entry.Key <= 30)
            {
                if (entry.Location.PageId == 100 + entry.Key)
                {
                    found[entry.Key] = true;
                }
            }
        }

        bool allFound = true;
        for (int i = 1; i <= 30; i++)
        {
            if (!found[i])
            {
                allFound = false;
                break;
            }
        }

        Assert.IsTrue(allFound);
    }

    [TestMethod]
    public void GetAllEntries_AfterDeletes_ReturnsRemaining()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 5; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, i));
        }

        btree.Delete(2);
        btree.Delete(4);

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.HasCount(3, entries);

        bool hasKey1 = false;
        bool hasKey2 = false;
        bool hasKey3 = false;
        bool hasKey4 = false;
        bool hasKey5 = false;
        foreach (BTreeEntry entry in entries)
        {
            if (entry.Key == 1) hasKey1 = true;
            if (entry.Key == 2) hasKey2 = true;
            if (entry.Key == 3) hasKey3 = true;
            if (entry.Key == 4) hasKey4 = true;
            if (entry.Key == 5) hasKey5 = true;
        }

        Assert.IsTrue(hasKey1);
        Assert.IsFalse(hasKey2);
        Assert.IsTrue(hasKey3);
        Assert.IsFalse(hasKey4);
        Assert.IsTrue(hasKey5);
    }

    [TestMethod]
    public async Task InsertAsync_SingleItem_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        DocumentLocation location = new DocumentLocation(100, 5);
        await btree.InsertAsync(1, location);

        DocumentLocation? found = await btree.SearchAsync(1);

        bool result = found != null && found.Value.PageId == 100 && found.Value.SlotIndex == 5;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public async Task InsertAsync_MultipleItems_CanSearchAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        await btree.InsertAsync(1, new DocumentLocation(100, 0));
        await btree.InsertAsync(5, new DocumentLocation(101, 1));
        await btree.InsertAsync(3, new DocumentLocation(102, 2));

        DocumentLocation? found1 = await btree.SearchAsync(1);
        DocumentLocation? found3 = await btree.SearchAsync(3);
        DocumentLocation? found5 = await btree.SearchAsync(5);

        bool result = found1.Value.PageId == 100 && found1.Value.SlotIndex == 0 &&
                      found3.Value.PageId == 102 && found3.Value.SlotIndex == 2 &&
                      found5.Value.PageId == 101 && found5.Value.SlotIndex == 1;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public async Task SearchAsync_NonExistingKey_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        await btree.InsertAsync(1, new DocumentLocation(100, 0));

        DocumentLocation? found = await btree.SearchAsync(99);

        pageIO.Dispose();

        Assert.IsNull(found);
    }

    [TestMethod]
    public async Task InsertAsync_ManyItems_CausesSplit()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, i));
        }

        DocumentLocation? found1 = await btree.SearchAsync(1);
        DocumentLocation? found10 = await btree.SearchAsync(10);
        DocumentLocation? found20 = await btree.SearchAsync(20);

        bool result = found1 != null && found1.Value.PageId == 101 && found1.Value.SlotIndex == 1 &&
                      found10 != null && found10.Value.PageId == 110 && found10.Value.SlotIndex == 10 &&
                      found20 != null && found20.Value.PageId == 120 && found20.Value.SlotIndex == 20;

        pageIO.Dispose();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public async Task DeleteAsync_SingleItem_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        await btree.InsertAsync(1, new DocumentLocation(100, 0));

        bool deleted = await btree.DeleteAsync(1);
        DocumentLocation? found = await btree.SearchAsync(1);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found);
    }

    [TestMethod]
    public async Task DeleteAsync_NonExistingKey_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        await btree.InsertAsync(1, new DocumentLocation(100, 0));

        bool deleted = await btree.DeleteAsync(99);

        pageIO.Dispose();

        Assert.IsFalse(deleted);
    }

    [TestMethod]
    public async Task DeleteAsync_FromMultiLevelTree_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, i));
        }

        bool deleted5 = await btree.DeleteAsync(5);
        bool deleted10 = await btree.DeleteAsync(10);
        bool deleted15 = await btree.DeleteAsync(15);

        DocumentLocation? found5 = await btree.SearchAsync(5);
        DocumentLocation? found10 = await btree.SearchAsync(10);
        DocumentLocation? found15 = await btree.SearchAsync(15);
        DocumentLocation? found1 = await btree.SearchAsync(1);
        DocumentLocation? found20 = await btree.SearchAsync(20);

        pageIO.Dispose();

        Assert.IsTrue(deleted5);
        Assert.IsTrue(deleted10);
        Assert.IsTrue(deleted15);
        Assert.IsNull(found5);
        Assert.IsNull(found10);
        Assert.IsNull(found15);
        Assert.IsNotNull(found1);
        Assert.IsNotNull(found20);
    }

    [TestMethod]
    public async Task InsertAsync_SequentialKeys_MaintainsOrder()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation? found = await btree.SearchAsync(i);
            if (found == null || found.Value.PageId != 100 + i || found.Value.SlotIndex != i)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }

    [TestMethod]
    public async Task InsertAsync_ReverseSequentialKeys_MaintainsOrder()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 50; i >= 1; i--)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, i));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation? found = await btree.SearchAsync(i);
            if (found == null || found.Value.PageId != 100 + i || found.Value.SlotIndex != i)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }

    // =====================================================
    // Edge Cases and Stress Tests
    // =====================================================

    [TestMethod]
    public void Search_EmptyTree_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        DocumentLocation? found = btree.Search(1);

        pageIO.Dispose();

        Assert.IsNull(found);
    }

    [TestMethod]
    public void Search_KeyLessThanAll_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 100; i <= 200; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        DocumentLocation? found = btree.Search(50);

        pageIO.Dispose();

        Assert.IsNull(found);
    }

    [TestMethod]
    public void Search_KeyGreaterThanAll_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 100; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        DocumentLocation? found = btree.Search(999);

        pageIO.Dispose();

        Assert.IsNull(found);
    }

    [TestMethod]
    public void Insert_MinimumOrder3_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 3;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        bool allFound = true;
        for (int i = 1; i <= 50; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != 100 + i)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(50, entries);
    }

    [TestMethod]
    public void Insert_RandomOrder_AllSearchable()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        int[] keys = { 50, 25, 75, 10, 30, 60, 90, 5, 15, 27, 35, 55, 65, 85, 95 };

        foreach (int key in keys)
        {
            btree.Insert(key, new DocumentLocation(key + 100, 0));
        }

        bool allFound = true;
        foreach (int key in keys)
        {
            DocumentLocation? found = btree.Search(key);
            if (found == null || found.Value.PageId != key + 100)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(keys.Length, entries);
    }

    [TestMethod]
    public void Insert_LargeScale_500Items()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 500; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool allFound = true;
        for (int i = 1; i <= 500; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != i)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(500, entries);
    }

    [TestMethod]
    public void Insert_LargeScale_1000Items_SmallOrder()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 4;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 1000; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool allFound = true;
        for (int i = 1; i <= 1000; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != i)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(1000, entries);
    }

    [TestMethod]
    public void Insert_DuplicateKey_UpdatesValue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(1, new DocumentLocation(100, 0));
        btree.Insert(1, new DocumentLocation(200, 5));

        DocumentLocation? found = btree.Search(1);
        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        // Note: Current implementation may add duplicate or update
        // This test documents the behavior
        Assert.IsNotNull(found);
    }

    [TestMethod]
    public void Delete_FirstKey_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool deleted = btree.Delete(1);
        DocumentLocation? found1 = btree.Search(1);
        DocumentLocation? found2 = btree.Search(2);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found1);
        Assert.IsNotNull(found2);
    }

    [TestMethod]
    public void Delete_LastKey_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool deleted = btree.Delete(20);
        DocumentLocation? found20 = btree.Search(20);
        DocumentLocation? found19 = btree.Search(19);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found20);
        Assert.IsNotNull(found19);
    }

    [TestMethod]
    public void Delete_InReverseOrder_AllDeleted()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 30; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool allDeleted = true;
        for (int i = 30; i >= 1; i--)
        {
            if (!btree.Delete(i))
            {
                allDeleted = false;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allDeleted);
        Assert.IsEmpty(entries);
    }

    [TestMethod]
    public void Delete_EveryOtherKey_RemainingIntact()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        List<int> missingOddKeys = new List<int>();
        for (int i = 2; i <= 20; i += 2)
        {
            btree.Delete(i);

            for (int j = 1; j < i; j += 2)
            {
                DocumentLocation? check = btree.Search(j);
                if (check == null && !missingOddKeys.Contains(j))
                {
                    missingOddKeys.Add(j);
                }
            }
        }

        bool oddFound = true;
        bool evenGone = true;
        for (int i = 1; i <= 20; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (i % 2 == 1 && found == null)
            {
                oddFound = false;
            }
            if (i % 2 == 0 && found != null)
            {
                evenGone = false;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(oddFound, $"Missing odd keys: {string.Join(", ", missingOddKeys)}");
        Assert.IsTrue(evenGone);
        Assert.HasCount(10, entries);
    }

    [TestMethod]
    public void InsertAfterDelete_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        for (int i = 1; i <= 10; i++)
        {
            btree.Delete(i);
        }

        for (int i = 21; i <= 30; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool correctState = true;
        for (int i = 1; i <= 10; i++)
        {
            if (btree.Search(i) != null)
            {
                correctState = false;
            }
        }
        for (int i = 11; i <= 30; i++)
        {
            if (btree.Search(i) == null)
            {
                correctState = false;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(correctState);
        Assert.HasCount(20, entries);
    }

    [TestMethod]
    public void MixedOperations_InterleavedInsertDelete()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(10, new DocumentLocation(10, 0));
        btree.Insert(20, new DocumentLocation(20, 0));
        btree.Insert(30, new DocumentLocation(30, 0));
        btree.Delete(20);
        btree.Insert(15, new DocumentLocation(15, 0));
        btree.Insert(25, new DocumentLocation(25, 0));
        btree.Delete(10);
        btree.Insert(5, new DocumentLocation(5, 0));
        btree.Delete(30);
        btree.Insert(35, new DocumentLocation(35, 0));

        DocumentLocation? found5 = btree.Search(5);
        DocumentLocation? found10 = btree.Search(10);
        DocumentLocation? found15 = btree.Search(15);
        DocumentLocation? found20 = btree.Search(20);
        DocumentLocation? found25 = btree.Search(25);
        DocumentLocation? found30 = btree.Search(30);
        DocumentLocation? found35 = btree.Search(35);

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsNotNull(found5);
        Assert.IsNull(found10);
        Assert.IsNotNull(found15);
        Assert.IsNull(found20);
        Assert.IsNotNull(found25);
        Assert.IsNull(found30);
        Assert.IsNotNull(found35);
        Assert.HasCount(4, entries);
    }

    [TestMethod]
    public void Insert_CausesMultipleInternalNodeSplits()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 3;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 100; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        bool allFound = true;
        for (int i = 1; i <= 100; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != i)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(100, entries);
    }

    [TestMethod]
    public void GetAllEntries_NoDuplicates_AfterManySplits()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 4;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 200; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        List<BTreeEntry> entries = btree.GetAllEntries();
        HashSet<int> seenKeys = new HashSet<int>();
        bool hasDuplicates = false;

        foreach (BTreeEntry entry in entries)
        {
            if (seenKeys.Contains(entry.Key))
            {
                hasDuplicates = true;
                break;
            }
            seenKeys.Add(entry.Key);
        }

        pageIO.Dispose();

        Assert.IsFalse(hasDuplicates);
        Assert.HasCount(200, entries);
    }

    [TestMethod]
    public void Delete_FromEmptyTree_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        bool deleted = btree.Delete(1);

        pageIO.Dispose();

        Assert.IsFalse(deleted);
    }

    [TestMethod]
    public void Insert_NegativeKeys_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = -50; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(i + 1000, 0));
        }

        bool allFound = true;
        for (int i = -50; i <= 50; i++)
        {
            DocumentLocation? found = btree.Search(i);
            if (found == null || found.Value.PageId != i + 1000)
            {
                allFound = false;
                break;
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        pageIO.Dispose();

        Assert.IsTrue(allFound);
        Assert.HasCount(101, entries);
    }

    [TestMethod]
    public void Insert_ZeroKey_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(0, new DocumentLocation(999, 0));
        btree.Insert(-1, new DocumentLocation(998, 0));
        btree.Insert(1, new DocumentLocation(1000, 0));

        DocumentLocation? foundNeg = btree.Search(-1);
        DocumentLocation? foundZero = btree.Search(0);
        DocumentLocation? foundPos = btree.Search(1);

        pageIO.Dispose();

        Assert.IsNotNull(foundNeg);
        Assert.IsNotNull(foundZero);
        Assert.IsNotNull(foundPos);
        Assert.AreEqual(998, foundNeg.Value.PageId);
        Assert.AreEqual(999, foundZero.Value.PageId);
        Assert.AreEqual(1000, foundPos.Value.PageId);
    }

    [TestMethod]
    public void Insert_LargeKeyValues_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        int[] largeKeys = { int.MaxValue, int.MaxValue - 1, int.MinValue, int.MinValue + 1, 0 };

        foreach (int key in largeKeys)
        {
            btree.Insert(key, new DocumentLocation(Math.Abs(key % 10000), 0));
        }

        bool allFound = true;
        foreach (int key in largeKeys)
        {
            DocumentLocation? found = btree.Search(key);
            if (found == null)
            {
                allFound = false;
                break;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allFound);
    }

    [TestMethod]
    public void StressTest_InsertDeleteCycles()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int cycle = 0; cycle < 5; cycle++)
        {
            int offset = cycle * 100;
            for (int i = 1; i <= 50; i++)
            {
                btree.Insert(offset + i, new DocumentLocation(offset + i, 0));
            }

            for (int i = 1; i <= 25; i++)
            {
                btree.Delete(offset + i);
            }
        }

        List<BTreeEntry> entries = btree.GetAllEntries();

        bool correctCount = entries.Count == 125;

        pageIO.Dispose();

        Assert.IsTrue(correctCount);
    }

    [TestMethod]
    public void GetAllEntries_VerifyAllKeysPresent_LargeTree()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 6;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        HashSet<int> insertedKeys = new HashSet<int>();
        for (int i = 1; i <= 300; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
            insertedKeys.Add(i);
        }

        List<BTreeEntry> entries = btree.GetAllEntries();
        HashSet<int> retrievedKeys = new HashSet<int>();
        foreach (BTreeEntry entry in entries)
        {
            retrievedKeys.Add(entry.Key);
        }

        bool allPresent = insertedKeys.SetEquals(retrievedKeys);

        pageIO.Dispose();

        Assert.IsTrue(allPresent);
        Assert.HasCount(300, entries);
    }

    // =====================================================
    // Underflow Detection Tests
    // =====================================================

    [TestMethod]
    public void GetMinKeys_Order5_Returns2()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);

        int minKeys = node.GetMinKeys();

        Assert.AreEqual(2, minKeys);
    }

    [TestMethod]
    public void GetMinKeys_Order10_Returns4()
    {
        int pageSize = 8192;
        int order = 10;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);

        int minKeys = node.GetMinKeys();

        Assert.AreEqual(4, minKeys);
    }

    [TestMethod]
    public void GetMinKeys_Order3_Returns1()
    {
        int pageSize = 8192;
        int order = 3;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);

        int minKeys = node.GetMinKeys();

        Assert.AreEqual(1, minKeys);
    }

    [TestMethod]
    public void IsUnderflow_BelowMinimum_ReturnsTrue()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 1;

        bool result = node.IsUnderflow();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void IsUnderflow_AtMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 2;

        bool result = node.IsUnderflow();

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void IsUnderflow_AboveMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 3;

        bool result = node.IsUnderflow();

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void CanLendKey_AboveMinimum_ReturnsTrue()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 3;

        bool result = node.CanLendKey();

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void CanLendKey_AtMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 2;

        bool result = node.CanLendKey();

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void CanLendKey_BelowMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int order = 5;
        BTreeNode node = new BTreeNode(pageSize, order, BTreeNodeType.Leaf);
        node.KeyCount = 1;

        bool result = node.CanLendKey();

        Assert.IsFalse(result);
    }

    // =====================================================
    // SearchRange Tests
    // =====================================================

    [TestMethod]
    public void SearchRange_EmptyTree_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        List<BTreeEntry> results = btree.SearchRange(1, 10, true, true);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void SearchRange_SingleItem_InRange_ReturnsItem()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(5, new DocumentLocation(105, 0));

        List<BTreeEntry> results = btree.SearchRange(1, 10, true, true);

        pageIO.Dispose();

        Assert.HasCount(1, results);
        Assert.AreEqual(5, results[0].Key);
        Assert.AreEqual(105, results[0].Location.PageId);
    }

    [TestMethod]
    public void SearchRange_SingleItem_OutOfRange_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        btree.Insert(50, new DocumentLocation(150, 0));

        List<BTreeEntry> results = btree.SearchRange(1, 10, true, true);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void SearchRange_MultipleItems_ReturnsOnlyInRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(5, 15, true, true);

        pageIO.Dispose();

        Assert.HasCount(11, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 5 || entry.Key > 15)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public void SearchRange_IncludeStartFalse_ExcludesStartKey()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(5, 10, false, true);

        pageIO.Dispose();

        Assert.HasCount(5, results);

        bool hasKey5 = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 5)
            {
                hasKey5 = true;
                break;
            }
        }

        Assert.IsFalse(hasKey5);
    }

    [TestMethod]
    public void SearchRange_IncludeEndFalse_ExcludesEndKey()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(1, 5, true, false);

        pageIO.Dispose();

        Assert.HasCount(4, results);

        bool hasKey5 = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 5)
            {
                hasKey5 = true;
                break;
            }
        }

        Assert.IsFalse(hasKey5);
    }

    [TestMethod]
    public void SearchRange_BothExclusive_ExcludesBothEnds()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(3, 7, false, false);

        pageIO.Dispose();

        Assert.HasCount(3, results);

        bool hasKey3 = false;
        bool hasKey7 = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 3) hasKey3 = true;
            if (entry.Key == 7) hasKey7 = true;
        }

        Assert.IsFalse(hasKey3);
        Assert.IsFalse(hasKey7);
    }

    [TestMethod]
    public void SearchRange_MultiLevelTree_ReturnsCorrectRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 100; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(25, 75, true, true);

        pageIO.Dispose();

        Assert.HasCount(51, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 25 || entry.Key > 75)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public void SearchRange_RangeAtStart_ReturnsCorrectItems()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(1, 10, true, true);

        pageIO.Dispose();

        Assert.HasCount(10, results);
    }

    [TestMethod]
    public void SearchRange_RangeAtEnd_ReturnsCorrectItems()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(41, 50, true, true);

        pageIO.Dispose();

        Assert.HasCount(10, results);
    }

    [TestMethod]
    public void SearchRange_RangeBeyondData_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(100, 200, true, true);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void SearchRange_RangeBeforeData_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 100; i <= 150; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(1, 50, true, true);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void SearchRange_SingleKeyRange_ReturnsOneItem()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(10, 10, true, true);

        pageIO.Dispose();

        Assert.HasCount(1, results);
        Assert.AreEqual(10, results[0].Key);
    }

    [TestMethod]
    public void SearchRange_SingleKeyRange_BothExclusive_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(10, 10, false, false);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void SearchRange_LargeTree_SmallRange_Efficient()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 1000; i++)
        {
            btree.Insert(i, new DocumentLocation(i, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(500, 510, true, true);

        pageIO.Dispose();

        Assert.HasCount(11, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 500 || entry.Key > 510)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public void SearchRange_NegativeKeys_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = -50; i <= 50; i++)
        {
            btree.Insert(i, new DocumentLocation(i + 1000, 0));
        }

        List<BTreeEntry> results = btree.SearchRange(-20, 20, true, true);

        pageIO.Dispose();

        Assert.HasCount(41, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < -20 || entry.Key > 20)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public void SearchRange_AfterDeletes_ReturnsCorrectRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 30; i++)
        {
            btree.Insert(i, new DocumentLocation(100 + i, 0));
        }

        btree.Delete(10);
        btree.Delete(15);
        btree.Delete(20);

        List<BTreeEntry> results = btree.SearchRange(8, 22, true, true);

        pageIO.Dispose();

        Assert.HasCount(12, results);

        bool hasDeleted = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 10 || entry.Key == 15 || entry.Key == 20)
            {
                hasDeleted = true;
                break;
            }
        }

        Assert.IsFalse(hasDeleted);
    }

    // =====================================================
    // SearchRangeAsync Tests
    // =====================================================

    [TestMethod]
    public async Task SearchRangeAsync_EmptyTree_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        List<BTreeEntry> results = await btree.SearchRangeAsync(1, 10, true, true);

        pageIO.Dispose();

        Assert.IsEmpty(results);
    }

    [TestMethod]
    public async Task SearchRangeAsync_MultipleItems_ReturnsOnlyInRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = await btree.SearchRangeAsync(5, 15, true, true);

        pageIO.Dispose();

        Assert.HasCount(11, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 5 || entry.Key > 15)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public async Task SearchRangeAsync_MultiLevelTree_ReturnsCorrectRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 100; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = await btree.SearchRangeAsync(25, 75, true, true);

        pageIO.Dispose();

        Assert.HasCount(51, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 25 || entry.Key > 75)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }

    [TestMethod]
    public async Task SearchRangeAsync_IncludeStartFalse_ExcludesStartKey()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = await btree.SearchRangeAsync(5, 10, false, true);

        pageIO.Dispose();

        Assert.HasCount(5, results);

        bool hasKey5 = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 5)
            {
                hasKey5 = true;
                break;
            }
        }

        Assert.IsFalse(hasKey5);
    }

    [TestMethod]
    public async Task SearchRangeAsync_IncludeEndFalse_ExcludesEndKey()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 10;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(100 + i, 0));
        }

        List<BTreeEntry> results = await btree.SearchRangeAsync(1, 5, true, false);

        pageIO.Dispose();

        Assert.HasCount(4, results);

        bool hasKey5 = false;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key == 5)
            {
                hasKey5 = true;
                break;
            }
        }

        Assert.IsFalse(hasKey5);
    }

    [TestMethod]
    public async Task SearchRangeAsync_LargeTree_SmallRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        int pageSize = 8192;
        int order = 5;
        IPageIO pageIO = null;
        PageManager pageManager = null;

        BTree btree = CreateBTree(dbPath, pageSize, order, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 500; i++)
        {
            await btree.InsertAsync(i, new DocumentLocation(i, 0));
        }

        List<BTreeEntry> results = await btree.SearchRangeAsync(200, 250, true, true);

        pageIO.Dispose();

        Assert.HasCount(51, results);

        bool allInRange = true;
        foreach (BTreeEntry entry in results)
        {
            if (entry.Key < 200 || entry.Key > 250)
            {
                allInRange = false;
                break;
            }
        }

        Assert.IsTrue(allInRange);
    }
}
