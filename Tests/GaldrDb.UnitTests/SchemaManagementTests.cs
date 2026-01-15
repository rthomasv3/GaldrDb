using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class SchemaManagementTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbSchemaTests_{Guid.NewGuid()}");
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

    #region GetCollectionNames Tests

    [TestMethod]
    public void GetCollectionNames_ReturnsRegisteredCollections()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            IReadOnlyList<string> names = db.GetCollectionNames();

            Assert.IsTrue(names.Contains("Person"));
            Assert.IsTrue(names.Contains("Customer"));
        }
    }

    #endregion

    #region GetIndexNames Tests

    [TestMethod]
    public void GetIndexNames_CollectionWithIndex_ReturnsIndexNames()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            IReadOnlyList<string> indexNames = db.GetIndexNames("Person");

            Assert.HasCount(1, indexNames);
            Assert.AreEqual("Name", indexNames[0]);
        }
    }

    [TestMethod]
    public void GetIndexNames_NonExistentCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                db.GetIndexNames("NonExistent");
            });
        }
    }

    #endregion

    #region DropIndex Tests

    [TestMethod]
    public void DropIndex_ExistingIndex_RemovesIndex()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            IReadOnlyList<string> indexesBefore = db.GetIndexNames("Person");
            Assert.HasCount(1, indexesBefore);

            db.DropIndex("Person", "Name");

            IReadOnlyList<string> indexesAfter = db.GetIndexNames("Person");
            Assert.IsEmpty(indexesAfter);
        }
    }

    [TestMethod]
    public void DropIndex_NonExistentCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                db.DropIndex("NonExistent", "SomeField");
            });
        }
    }

    [TestMethod]
    public void DropIndex_NonExistentIndex_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            Assert.Throws<InvalidOperationException>(() =>
            {
                db.DropIndex("Person", "NonExistentField");
            });
        }
    }

    #endregion

    #region DropCollection Tests

    [TestMethod]
    public void DropCollection_EmptyCollection_RemovesCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            int countBefore = db.GetCollectionNames().Count;

            Person person = db.GetById<Person>(1);
            db.Delete<Person>(person.Id);

            db.DropCollection("Person");

            int countAfter = db.GetCollectionNames().Count;
            Assert.AreEqual(countBefore - 1, countAfter);
            Assert.IsFalse(db.GetCollectionNames().Contains("Person"));
        }
    }

    [TestMethod]
    public void DropCollection_NonExistentCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                db.DropCollection("NonExistent");
            });
        }
    }

    [TestMethod]
    public void DropCollection_WithDocuments_WithoutDeleteFlag_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
            {
                db.DropCollection("Person");
            });

            Assert.Contains("1 document(s)", ex.Message);
            Assert.Contains("deleteDocuments", ex.Message);
        }
    }

    [TestMethod]
    public void DropCollection_WithDocuments_WithDeleteFlag_RemovesCollectionAndDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test1", Age = 25, Email = "test1@example.com" });
            db.Insert(new Person { Name = "Test2", Age = 30, Email = "test2@example.com" });

            int countBefore = db.GetCollectionNames().Count;

            db.DropCollection("Person", deleteDocuments: true);

            int countAfter = db.GetCollectionNames().Count;
            Assert.AreEqual(countBefore - 1, countAfter);
            Assert.IsFalse(db.GetCollectionNames().Contains("Person"));
        }
    }

    [TestMethod]
    public void DropCollection_AlsoDropsIndexes()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            IReadOnlyList<string> indexesBefore = db.GetIndexNames("Person");
            Assert.HasCount(1, indexesBefore);

            db.DropCollection("Person", deleteDocuments: true);

            Assert.Throws<InvalidOperationException>(() =>
            {
                db.GetIndexNames("Person");
            });
        }
    }

    #endregion
}
