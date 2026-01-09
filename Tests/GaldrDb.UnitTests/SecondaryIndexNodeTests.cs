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

        SecondaryIndexNode deserialized = SecondaryIndexNode.Deserialize(buffer, pageSize);

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

        SecondaryIndexNode deserialized = SecondaryIndexNode.Deserialize(buffer, pageSize);

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

        SecondaryIndexNode deserialized = SecondaryIndexNode.Deserialize(buffer, pageSize);

        Assert.AreEqual(maxKeys, writtenMaxKeys);
    }

    [TestMethod]
    public void EnsureCollection_CreatesIndex_InsertWorks()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };

            int id = db.Insert(person, PersonMeta.TypeInfo);

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

        SecondaryIndexNode readNode = SecondaryIndexNode.Deserialize(readBuffer, pageSize);

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
        pageManager.Initialize();

        int rootPageId = pageManager.AllocatePage();
        SecondaryIndexNode rootNode = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
        byte[] rootBuffer = new byte[pageSize];
        rootNode.SerializeTo(rootBuffer);
        pageIO.WritePage(rootPageId, rootBuffer);

        SecondaryIndexBTree tree = new SecondaryIndexBTree(pageIO, pageManager, rootPageId, pageSize, maxKeys);

        byte[] key = Encoding.UTF8.GetBytes("TestKey");
        byte[] compositeKey = SecondaryIndexBTree.CreateCompositeKey(key, 1);
        DocumentLocation location = new DocumentLocation(10, 0);

        tree.Insert(compositeKey, location);

        DocumentLocation found = tree.Search(compositeKey);

        pageIO.Close();
        pageIO.Dispose();

        Assert.IsNotNull(found);
        Assert.AreEqual(10, found.PageId);
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
        SecondaryIndexNode readNode = SecondaryIndexNode.Deserialize(readBuffer, pageSize);

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
            db.EnsureCollection(PersonMeta.TypeInfo);

            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" };
                int id = db.Insert(person, PersonMeta.TypeInfo);
                Assert.AreEqual(i + 1, id, $"Expected id {i + 1}, got {id}");
            }

            Person retrieved = db.GetById(5, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Person4", retrieved.Name);
        }
    }
}
