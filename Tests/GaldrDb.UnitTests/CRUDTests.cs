using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class CRUDTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbCRUDTests_{Guid.NewGuid()}");
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
    public void DeleteDocument_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            int docId = db.InsertDocument("people", person);

            bool deleted = db.DeleteDocument("people", docId);

            Assert.IsTrue(deleted);
        }
    }

    [TestMethod]
    public void DeleteDocument_DocumentNoLongerRetrievable()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            int docId = db.InsertDocument("people", person);

            db.DeleteDocument("people", docId);

            Person retrieved = db.GetDocument<Person>("people", docId);

            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public void DeleteDocument_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            bool deleted = db.DeleteDocument("people", 999);

            Assert.IsFalse(deleted);
        }
    }

    [TestMethod]
    public void DeleteDocument_CollectionNotFound_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            try
            {
                db.DeleteDocument("nonexistent", 1);
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void DeleteDocument_OtherDocumentsUnaffected()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            Person person1 = new Person { Name = "Alice", Age = 25, Email = "alice@example.com" };
            Person person2 = new Person { Name = "Bob", Age = 35, Email = "bob@example.com" };
            Person person3 = new Person { Name = "Charlie", Age = 45, Email = "charlie@example.com" };

            int id1 = db.InsertDocument("people", person1);
            int id2 = db.InsertDocument("people", person2);
            int id3 = db.InsertDocument("people", person3);

            db.DeleteDocument("people", id2);

            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);
            Person retrieved3 = db.GetDocument<Person>("people", id3);

            bool result = retrieved1 != null && retrieved1.Name == "Alice" &&
                          retrieved2 == null &&
                          retrieved3 != null && retrieved3.Name == "Charlie";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void UpdateDocument_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            int docId = db.InsertDocument("people", person);

            Person updated = new Person { Name = "John Doe Updated", Age = 31, Email = "john.updated@example.com" };
            bool result = db.UpdateDocument("people", docId, updated);

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void UpdateDocument_RetrievesUpdatedData()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            int docId = db.InsertDocument("people", person);

            Person updated = new Person { Name = "Jane Doe", Age = 28, Email = "jane@example.com" };
            db.UpdateDocument("people", docId, updated);

            Person retrieved = db.GetDocument<Person>("people", docId);

            bool result = retrieved.Name == "Jane Doe" &&
                          retrieved.Age == 28 &&
                          retrieved.Email == "jane@example.com";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void UpdateDocument_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            Person updated = new Person { Name = "Ghost", Age = 0, Email = "ghost@example.com" };
            bool result = db.UpdateDocument("people", 999, updated);

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void UpdateDocument_CollectionNotFound_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            try
            {
                Person updated = new Person { Name = "Ghost", Age = 0, Email = "ghost@example.com" };
                db.UpdateDocument("nonexistent", 1, updated);
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void UpdateDocument_WithLargerData_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "Short", Age = 30, Email = "s@e.com" };
            int docId = db.InsertDocument("people", person);

            Person updated = new Person
            {
                Name = "This is a much longer name that takes up significantly more space in storage",
                Age = 99,
                Email = "verylongemailaddress_that_nobody_would_ever_use@extremelylongdomainname.example.com"
            };
            db.UpdateDocument("people", docId, updated);

            Person retrieved = db.GetDocument<Person>("people", docId);

            bool result = retrieved.Name.StartsWith("This is a much longer name");

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void UpdateDocument_PersistsAfterReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int docId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "Original", Age = 30, Email = "original@example.com" };
            docId = db.InsertDocument("people", person);

            Person updated = new Person { Name = "Updated", Age = 35, Email = "updated@example.com" };
            db.UpdateDocument("people", docId, updated);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved = db.GetDocument<Person>("people", docId);

            bool result = retrieved.Name == "Updated" && retrieved.Age == 35;

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void DeleteThenInsert_NewDocumentGetsNewId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            Person person1 = new Person { Name = "First", Age = 25, Email = "first@example.com" };
            int id1 = db.InsertDocument("people", person1);

            db.DeleteDocument("people", id1);

            Person person2 = new Person { Name = "Second", Age = 30, Email = "second@example.com" };
            int id2 = db.InsertDocument("people", person2);

            bool result = id2 > id1;

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void BulkDelete_Insert100Delete50_Remaining50Queryable()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            int[] docIds = new int[100];

            for (int i = 0; i < 100; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                docIds[i] = db.InsertDocument("people", person);
            }

            for (int i = 0; i < 50; i++)
            {
                db.DeleteDocument("people", docIds[i]);
            }

            int foundCount = 0;
            int deletedCount = 0;

            for (int i = 0; i < 100; i++)
            {
                Person retrieved = db.GetDocument<Person>("people", docIds[i]);
                if (i < 50)
                {
                    if (retrieved == null)
                    {
                        deletedCount++;
                    }
                }
                else
                {
                    if (retrieved != null && retrieved.Name == $"Person {i}")
                    {
                        foundCount++;
                    }
                }
            }

            bool result = deletedCount == 50 && foundCount == 50;

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void DeleteDocument_PersistsAfterReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int docId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "ToDelete", Age = 30, Email = "delete@example.com" };
            docId = db.InsertDocument("people", person);

            db.DeleteDocument("people", docId);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved = db.GetDocument<Person>("people", docId);

            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public void UpdateMultipleDocuments_AllUpdatesApplied()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            int[] docIds = new int[10];
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Original {i}", Age = 20 + i, Email = $"original{i}@example.com" };
                docIds[i] = db.InsertDocument("people", person);
            }

            for (int i = 0; i < 10; i++)
            {
                Person updated = new Person { Name = $"Updated {i}", Age = 30 + i, Email = $"updated{i}@example.com" };
                db.UpdateDocument("people", docIds[i], updated);
            }

            bool allUpdated = true;
            for (int i = 0; i < 10; i++)
            {
                Person retrieved = db.GetDocument<Person>("people", docIds[i]);
                if (retrieved == null || retrieved.Name != $"Updated {i}" || retrieved.Age != 30 + i)
                {
                    allUpdated = false;
                    break;
                }
            }

            Assert.IsTrue(allUpdated);
        }
    }
}
