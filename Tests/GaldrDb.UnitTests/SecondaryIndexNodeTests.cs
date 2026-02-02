using System;
using System.IO;
using System.Text;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class SecondaryIndexNodeTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbSecondaryIndexTests_{Guid.NewGuid()}");
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

    [TestMethod]
    public void CalculateMaxKeys_Returns_ReasonableValue()
    {
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        Assert.IsGreaterThan(0, maxKeys);
        Assert.IsLessThan(1000, maxKeys);
    }

    [TestMethod]
    public void NewNode_IsNotFull()
    {
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);

        Assert.AreEqual(0, node.KeyCount);
        Assert.IsFalse(node.IsFull());
    }

    [TestMethod]
    public void Serialize_Deserialize_PreservesKeyCount()
    {
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] buffer = new byte[pageSize];
        node.SerializeTo(buffer);

        SecondaryIndexNode deserialized = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode.DeserializeTo(buffer, deserialized);

        Assert.AreEqual(0, deserialized.KeyCount);
    }

    [TestMethod]
    public void Serialize_Deserialize_PreservesIsFull()
    {
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] buffer = new byte[pageSize];
        node.SerializeTo(buffer);

        SecondaryIndexNode deserialized = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode.DeserializeTo(buffer, deserialized);

        Assert.IsFalse(deserialized.IsFull());
    }

    [TestMethod]
    public void Deserialize_ReadsMaxKeysCorrectly()
    {
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] buffer = new byte[pageSize];
        node.SerializeTo(buffer);

        ushort writtenMaxKeys = (ushort)(buffer[8] | (buffer[9] << 8));

        SecondaryIndexNode deserialized = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode.DeserializeTo(buffer, deserialized);

        Assert.AreEqual(maxKeys, writtenMaxKeys);
    }

    [TestMethod]
    public void EnsureCollection_CreatesIndex_InsertWorks()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };

            int id = db.Insert(person);

            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void PageIO_WriteRead_PreservesNodeData()
    {
        string filePath = Path.Combine(_testDirectory, "test_pageio.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true);

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] writeBuffer = new byte[pageSize];
        node.SerializeTo(writeBuffer);

        pageIO.WritePage(0, writeBuffer);

        byte[] readBuffer = new byte[pageSize];
        pageIO.ReadPage(0, readBuffer);

        SecondaryIndexNode readNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode.DeserializeTo(readBuffer, readNode);

        pageIO.Close();
        pageIO.Dispose();

        Assert.AreEqual(0, readNode.KeyCount);
        Assert.IsFalse(readNode.IsFull());
    }

    [TestMethod]
    public void SecondaryIndexBTree_Insert_SingleKey_Works()
    {
        string filePath = Path.Combine(_testDirectory, "test_index.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true);
        PageManager pageManager = new PageManager(pageIO, pageSize);
        PageLockManager pageLockManager = new PageLockManager();
        
        pageManager.Initialize();

        int rootPageId = pageManager.AllocatePage();
        SecondaryIndexNode rootNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] rootBuffer = new byte[pageSize];
        rootNode.SerializeTo(rootBuffer);
        pageIO.WritePage(rootPageId, rootBuffer);

        SecondaryIndexBTree tree = new SecondaryIndexBTree(pageIO, pageManager, pageLockManager, rootPageId, pageSize, pageSize, maxKeys);

        byte[] key = Encoding.UTF8.GetBytes("TestKey");
        byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, 1);
        DocumentLocation location = new DocumentLocation(10, 0);

        tree.Insert(compositeKey, location);

        DocumentLocation? found = tree.Search(compositeKey);

        pageIO.Close();
        pageIO.Dispose();

        Assert.IsNotNull(found);
        Assert.AreEqual(10, found.Value.PageId);
    }

    [TestMethod]
    public void SecondaryIndexBTree_ReadPageAfterWrite_NodeIsNotFull()
    {
        string filePath = Path.Combine(_testDirectory, "test_index2.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        StandardPageIO pageIO = new StandardPageIO(filePath, pageSize, true);
        PageManager pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();

        int rootPageId = pageManager.AllocatePage();
        SecondaryIndexNode rootNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] rootBuffer = new byte[pageSize];
        rootNode.SerializeTo(rootBuffer);
        pageIO.WritePage(rootPageId, rootBuffer);

        byte[] readBuffer = new byte[pageSize];
        pageIO.ReadPage(rootPageId, readBuffer);
        SecondaryIndexNode readNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode.DeserializeTo(readBuffer, readNode);

        pageIO.Close();
        pageIO.Dispose();

        Assert.AreEqual(0, readNode.KeyCount);
        Assert.IsFalse(readNode.IsFull());
    }

    [TestMethod]
    public void SecondaryIndex_MultipleInserts_DoNotCorruptPrimaryIndex()
    {
        string dbPath = Path.Combine(_testDirectory, "multi_insert.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" };
                int id = db.Insert(person);
                Assert.AreEqual(i + 1, id, $"Expected id {i + 1}, got {id}");
            }

            Person retrieved = db.GetById<Person>(5);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Person4", retrieved.Name);
        }
    }

    #region Underflow Detection Tests

    [TestMethod]
    public void GetMinKeys_ReturnsHalfOfMaxKeys()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);

        Assert.AreEqual(5, node.GetMinKeys());
    }

    [TestMethod]
    public void GetMinKeys_OddMaxKeys_ReturnsFloorHalf()
    {
        int pageSize = 8192;
        int maxKeys = 11;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);

        Assert.AreEqual(5, node.GetMinKeys());
    }

    [TestMethod]
    public void IsUnderflow_BelowMinimum_ReturnsTrue()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 4;

        Assert.IsTrue(node.IsUnderflow());
    }

    [TestMethod]
    public void IsUnderflow_AtMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 5;

        Assert.IsFalse(node.IsUnderflow());
    }

    [TestMethod]
    public void IsUnderflow_AboveMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 7;

        Assert.IsFalse(node.IsUnderflow());
    }

    [TestMethod]
    public void CanLendKey_AboveMinimum_ReturnsTrue()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 6;

        Assert.IsTrue(node.CanLendKey());
    }

    [TestMethod]
    public void CanLendKey_AtMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 5;

        Assert.IsFalse(node.CanLendKey());
    }

    [TestMethod]
    public void CanLendKey_BelowMinimum_ReturnsFalse()
    {
        int pageSize = 8192;
        int maxKeys = 10;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        node.KeyCount = 3;

        Assert.IsFalse(node.CanLendKey());
    }

    #endregion

    #region SecondaryIndexBTree Delete Tests

    private SecondaryIndexBTree CreateSecondaryIndexBTree(string dbPath, int pageSize, int maxKeys, out IPageIO pageIO, out PageManager pageManager, out PageLockManager pageLockManager)
    {
        pageIO = new StandardPageIO(dbPath, pageSize, true);
        pageManager = new PageManager(pageIO, pageSize);
        pageManager.Initialize();
        pageLockManager = new PageLockManager();

        int rootPageId = pageManager.AllocatePage();
        SecondaryIndexNode rootNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] rootBuffer = new byte[pageSize];
        rootNode.SerializeTo(rootBuffer);
        pageIO.WritePage(rootPageId, rootBuffer);
        pageIO.Flush();

        return new SecondaryIndexBTree(pageIO, pageManager, pageLockManager, rootPageId, pageSize, pageSize, maxKeys);
    }

    [TestMethod]
    public void SecondaryIndexBTree_Delete_SingleItem_Success()
    {
        string filePath = Path.Combine(_testDirectory, "delete_single.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        byte[] key = Encoding.UTF8.GetBytes("TestKey");
        byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, 1);
        tree.Insert(compositeKey, new DocumentLocation(10, 0));

        bool deleted = tree.Delete(compositeKey);
        DocumentLocation? found = tree.Search(compositeKey);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found);
    }

    [TestMethod]
    public void SecondaryIndexBTree_Delete_NonExistingKey_ReturnsFalse()
    {
        string filePath = Path.Combine(_testDirectory, "delete_nonexist.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        byte[] key = Encoding.UTF8.GetBytes("NonExistent");
        byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, 1);

        bool deleted = tree.Delete(compositeKey);

        pageIO.Dispose();

        Assert.IsFalse(deleted);
    }

    [TestMethod]
    public void SecondaryIndexBTree_Delete_FromMultipleItems_OthersRemain()
    {
        string filePath = Path.Combine(_testDirectory, "delete_multi.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 5; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        byte[] deleteKey = Encoding.UTF8.GetBytes("Key3");
        byte[] deleteCompositeKey = SecondaryIndexBTree.CreateCompositeKey(deleteKey, 3);
        bool deleted = tree.Delete(deleteCompositeKey);

        DocumentLocation? found3 = tree.Search(deleteCompositeKey);

        byte[] key1 = Encoding.UTF8.GetBytes("Key1");
        byte[] compositeKey1 = SecondaryIndexBTree.CreateCompositeKey(key1, 1);
        DocumentLocation? found1 = tree.Search(compositeKey1);

        byte[] key5 = Encoding.UTF8.GetBytes("Key5");
        byte[] compositeKey5 = SecondaryIndexBTree.CreateCompositeKey(key5, 5);
        DocumentLocation? found5 = tree.Search(compositeKey5);

        pageIO.Dispose();

        Assert.IsTrue(deleted);
        Assert.IsNull(found3);
        Assert.IsNotNull(found1);
        Assert.IsNotNull(found5);
    }

    [TestMethod]
    public void SecondaryIndexBTree_Delete_FromMultiLevelTree_Success()
    {
        string filePath = Path.Combine(_testDirectory, "delete_multilevel.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        byte[] deleteKey5 = Encoding.UTF8.GetBytes("Key005");
        byte[] deleteCompositeKey5 = SecondaryIndexBTree.CreateCompositeKey(deleteKey5, 5);
        bool deleted5 = tree.Delete(deleteCompositeKey5);

        byte[] deleteKey10 = Encoding.UTF8.GetBytes("Key010");
        byte[] deleteCompositeKey10 = SecondaryIndexBTree.CreateCompositeKey(deleteKey10, 10);
        bool deleted10 = tree.Delete(deleteCompositeKey10);

        byte[] deleteKey15 = Encoding.UTF8.GetBytes("Key015");
        byte[] deleteCompositeKey15 = SecondaryIndexBTree.CreateCompositeKey(deleteKey15, 15);
        bool deleted15 = tree.Delete(deleteCompositeKey15);

        DocumentLocation? found5 = tree.Search(deleteCompositeKey5);
        DocumentLocation? found10 = tree.Search(deleteCompositeKey10);
        DocumentLocation? found15 = tree.Search(deleteCompositeKey15);

        byte[] key1 = Encoding.UTF8.GetBytes("Key001");
        byte[] compositeKey1 = SecondaryIndexBTree.CreateCompositeKey(key1, 1);
        DocumentLocation? found1 = tree.Search(compositeKey1);

        byte[] key20 = Encoding.UTF8.GetBytes("Key020");
        byte[] compositeKey20 = SecondaryIndexBTree.CreateCompositeKey(key20, 20);
        DocumentLocation? found20 = tree.Search(compositeKey20);

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
    public void SecondaryIndexBTree_Delete_EveryOtherKey_RemainingIntact()
    {
        string filePath = Path.Combine(_testDirectory, "delete_everyother.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 20; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        for (int i = 2; i <= 20; i += 2)
        {
            byte[] deleteKey = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] deleteCompositeKey = SecondaryIndexBTree.CreateCompositeKey(deleteKey, i);

            DocumentLocation? beforeDelete = tree.Search(deleteCompositeKey);
            Console.WriteLine($"Key {i} before delete: {(beforeDelete != null ? "found" : "NOT FOUND")}");

            bool deleted = tree.Delete(deleteCompositeKey);
            Console.WriteLine($"Key {i} delete returned: {deleted}");

            Assert.IsTrue(deleted, $"Failed to delete key {i}, search before delete: {(beforeDelete != null ? "found" : "NOT FOUND")}");
        }

        bool allOddFound = true;
        for (int i = 1; i <= 19; i += 2)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation? found = tree.Search(compositeKey);
            if (found == null)
            {
                allOddFound = false;
            }
        }

        bool allEvenGone = true;
        for (int i = 2; i <= 20; i += 2)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation? found = tree.Search(compositeKey);
            if (found != null)
            {
                allEvenGone = false;
            }
        }

        pageIO.Dispose();

        Assert.IsTrue(allOddFound, "Some odd keys are missing after delete");
        Assert.IsTrue(allEvenGone, "Some even keys still exist after delete");
    }

    [TestMethod]
    public void SecondaryIndexBTree_StressTest_InsertDeleteCycles()
    {
        string filePath = Path.Combine(_testDirectory, "stress_test.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 100; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D4}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        for (int i = 1; i <= 75; i++)
        {
            byte[] deleteKey = Encoding.UTF8.GetBytes($"Key{i:D4}");
            byte[] deleteCompositeKey = SecondaryIndexBTree.CreateCompositeKey(deleteKey, i);
            tree.Delete(deleteCompositeKey);
        }

        for (int i = 101; i <= 150; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D4}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        int foundCount = 0;
        for (int i = 76; i <= 150; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D4}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation? found = tree.Search(compositeKey);
            if (found != null)
            {
                foundCount++;
            }
        }

        pageIO.Dispose();

        Assert.AreEqual(75, foundCount, $"Expected 75 entries, found {foundCount}");
    }

    [TestMethod]
    public void SecondaryIndexBTree_Delete_AllItems_TreeEmpty()
    {
        string filePath = Path.Combine(_testDirectory, "delete_all.db");
        int pageSize = 8192;
        int maxKeys = 5;

        IPageIO pageIO = null;
        PageManager pageManager = null;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(filePath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 10; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            tree.Insert(compositeKey, new DocumentLocation(100 + i, i));
        }

        for (int i = 1; i <= 10; i++)
        {
            byte[] deleteKey = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] deleteCompositeKey = SecondaryIndexBTree.CreateCompositeKey(deleteKey, i);
            bool deleted = tree.Delete(deleteCompositeKey);
            Assert.IsTrue(deleted, $"Failed to delete key {i}");
        }

        for (int i = 1; i <= 10; i++)
        {
            byte[] key = Encoding.UTF8.GetBytes($"Key{i:D3}");
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation? found = tree.Search(compositeKey);
            Assert.IsNull(found, $"Key {i} should not exist after deletion");
        }

        pageIO.Dispose();
    }

    #endregion

    #region Large Key Stress Tests

    [TestMethod]
    public void SecondaryIndexBTree_LargeKeys_ManyInserts_NoOverflow()
    {
        string dbPath = Path.Combine(_testDirectory, "large_keys_insert.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        IPageIO pageIO;
        PageManager pageManager;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(dbPath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 200; i++)
        {
            string largeKeyString = $"VeryLongKeyNameThatExceedsTheAssumedAverageKeySize_{i:D5}_ExtraDataToMakeItEvenLonger";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation location = new DocumentLocation(i, i % 10);

            tree.Insert(compositeKey, location);
        }

        for (int i = 1; i <= 200; i++)
        {
            string largeKeyString = $"VeryLongKeyNameThatExceedsTheAssumedAverageKeySize_{i:D5}_ExtraDataToMakeItEvenLonger";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);

            DocumentLocation? found = tree.Search(compositeKey);
            Assert.IsNotNull(found, $"Key {i} should exist");
            Assert.AreEqual(i, found.Value.PageId);
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void SecondaryIndexBTree_LargeKeys_InsertAndDelete_NoOverflow()
    {
        string dbPath = Path.Combine(_testDirectory, "large_keys_delete.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        IPageIO pageIO;
        PageManager pageManager;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(dbPath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 300; i++)
        {
            string largeKeyString = $"LargeKey_{i:D5}_WithLotsOfExtraCharactersToExceedNormalKeySize";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation location = new DocumentLocation(i, 0);

            tree.Insert(compositeKey, location);
        }

        for (int i = 1; i <= 250; i++)
        {
            string largeKeyString = $"LargeKey_{i:D5}_WithLotsOfExtraCharactersToExceedNormalKeySize";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);

            bool deleted = tree.Delete(compositeKey);
            Assert.IsTrue(deleted, $"Failed to delete key {i}");
        }

        for (int i = 251; i <= 300; i++)
        {
            string largeKeyString = $"LargeKey_{i:D5}_WithLotsOfExtraCharactersToExceedNormalKeySize";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);

            DocumentLocation? found = tree.Search(compositeKey);
            Assert.IsNotNull(found, $"Key {i} should still exist");
        }

        for (int i = 1; i <= 250; i++)
        {
            string largeKeyString = $"LargeKey_{i:D5}_WithLotsOfExtraCharactersToExceedNormalKeySize";
            byte[] key = Encoding.UTF8.GetBytes(largeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);

            DocumentLocation? found = tree.Search(compositeKey);
            Assert.IsNull(found, $"Key {i} should be deleted");
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void SecondaryIndexBTree_VeryLargeKeys_HandledCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "very_large_keys.db");
        int pageSize = 8192;
        int maxKeys = SecondaryIndexBTree.CalculateMaxKeys(pageSize);

        IPageIO pageIO;
        PageManager pageManager;
        SecondaryIndexBTree tree = CreateSecondaryIndexBTree(dbPath, pageSize, maxKeys, out pageIO, out pageManager, out PageLockManager _);

        for (int i = 1; i <= 50; i++)
        {
            string veryLargeKeyString = new string('X', 150) + $"_{i:D5}";
            byte[] key = Encoding.UTF8.GetBytes(veryLargeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);
            DocumentLocation location = new DocumentLocation(i, 0);

            tree.Insert(compositeKey, location);
        }

        for (int i = 1; i <= 50; i++)
        {
            string veryLargeKeyString = new string('X', 150) + $"_{i:D5}";
            byte[] key = Encoding.UTF8.GetBytes(veryLargeKeyString);
            byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, i);

            DocumentLocation? found = tree.Search(compositeKey);
            Assert.IsNotNull(found, $"Very large key {i} should exist");
        }

        pageIO.Dispose();
    }

    [TestMethod]
    public void Database_LargeIndexedFieldValues_InsertDeleteCycle()
    {
        string dbPath = Path.Combine(_testDirectory, "large_field_cycle.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 500; i++)
            {
                Person person = new Person
                {
                    Name = $"Person_{i:D5}_WithAVeryLongNameToTestLargeIndexKeys",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@example.com"
                };
                db.Insert(person);
            }

            for (int i = 1; i <= 400; i++)
            {
                db.DeleteById<Person>(i);
            }

            for (int i = 401; i <= 500; i++)
            {
                Person person = db.GetById<Person>(i);
                Assert.IsNotNull(person, $"Person {i} should exist");
            }

            System.Collections.Generic.List<Person> remaining = db.Query<Person>().ToList();
            Assert.HasCount(100, remaining);
        }
    }

    #endregion
}
