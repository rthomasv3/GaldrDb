using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class TypeSafeCRUDTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbTypeSafeCRUDTests_{Guid.NewGuid()}");
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

    #region EnsureCollection Tests

    [TestMethod]
    public void EnsureCollection_CreatesNewCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Assert.IsGreaterThan(0, id);
        }
    }

    [TestMethod]
    public void EnsureCollection_CalledTwice_DoesNotThrow()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Assert.IsGreaterThan(0, id);
        }
    }

    [TestMethod]
    public void EnsureCollection_UsesGaldrCollectionAttributeName()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(LegacyCustomerMeta.TypeInfo);

            LegacyCustomer customer = new LegacyCustomer { Name = "Test Customer", Email = "customer@example.com" };
            int id = db.Insert(customer, LegacyCustomerMeta.TypeInfo);

            LegacyCustomer retrieved = db.GetDocument<LegacyCustomer>("Customer", id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test Customer", retrieved.Name);
        }
    }

    #endregion

    #region Insert Tests

    [TestMethod]
    public void Insert_WithZeroId_AutoAssignsId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Id = 0, Name = "Auto ID", Age = 30, Email = "auto@example.com" };
            int assignedId = db.Insert(person, PersonMeta.TypeInfo);

            Assert.AreEqual(1, assignedId);
            Assert.AreEqual(1, person.Id);
        }
    }

    [TestMethod]
    public void Insert_WithExplicitId_UsesProvidedId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Id = 100, Name = "Explicit ID", Age = 25, Email = "explicit@example.com" };
            int assignedId = db.Insert(person, PersonMeta.TypeInfo);

            Assert.AreEqual(100, assignedId);
            Assert.AreEqual(100, person.Id);
        }
    }

    [TestMethod]
    public void Insert_MultipleDocuments_AutoIncrementsIds()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person1 = new Person { Name = "First", Age = 20, Email = "first@example.com" };
            Person person2 = new Person { Name = "Second", Age = 25, Email = "second@example.com" };
            Person person3 = new Person { Name = "Third", Age = 30, Email = "third@example.com" };

            int id1 = db.Insert(person1, PersonMeta.TypeInfo);
            int id2 = db.Insert(person2, PersonMeta.TypeInfo);
            int id3 = db.Insert(person3, PersonMeta.TypeInfo);

            Assert.AreEqual(1, id1);
            Assert.AreEqual(2, id2);
            Assert.AreEqual(3, id3);
        }
    }

    [TestMethod]
    public void Insert_WithExplicitIdHigherThanNext_UpdatesNextId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person1 = new Person { Id = 50, Name = "High ID", Age = 30, Email = "high@example.com" };
            db.Insert(person1, PersonMeta.TypeInfo);

            Person person2 = new Person { Name = "Auto After High", Age = 25, Email = "auto@example.com" };
            int id2 = db.Insert(person2, PersonMeta.TypeInfo);

            Assert.AreEqual(51, id2);
        }
    }

    [TestMethod]
    public void Insert_WithoutEnsureCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            try
            {
                Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
                db.Insert(person, PersonMeta.TypeInfo);
            }
            catch (InvalidOperationException ex)
            {
                exceptionThrown = true;
                Assert.Contains("does not exist", ex.Message);
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    #endregion

    #region GetById Tests

    [TestMethod]
    public void GetById_ExistingDocument_ReturnsDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "John Doe", Age = 35, Email = "john@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Person retrieved = db.GetById(id, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("John Doe", retrieved.Name);
            Assert.AreEqual(35, retrieved.Age);
            Assert.AreEqual("john@example.com", retrieved.Email);
        }
    }

    [TestMethod]
    public void GetById_NonExistentDocument_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person retrieved = db.GetById(999, PersonMeta.TypeInfo);

            Assert.IsNull(retrieved);
        }
    }

    #endregion

    #region Update Tests

    [TestMethod]
    public void Update_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Original", Age = 30, Email = "original@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            person.Name = "Updated";
            person.Age = 35;
            bool result = db.Update(person, PersonMeta.TypeInfo);

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void Update_ExistingDocument_PersistsChanges()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Original", Age = 30, Email = "original@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            person.Name = "Updated";
            person.Age = 35;
            person.Email = "updated@example.com";
            db.Update(person, PersonMeta.TypeInfo);

            Person retrieved = db.GetById(id, PersonMeta.TypeInfo);

            Assert.AreEqual("Updated", retrieved.Name);
            Assert.AreEqual(35, retrieved.Age);
            Assert.AreEqual("updated@example.com", retrieved.Email);
        }
    }

    [TestMethod]
    public void Update_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Id = 999, Name = "Ghost", Age = 0, Email = "ghost@example.com" };
            bool result = db.Update(person, PersonMeta.TypeInfo);

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void Update_WithZeroId_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            try
            {
                Person person = new Person { Id = 0, Name = "Zero ID", Age = 25, Email = "zero@example.com" };
                db.Update(person, PersonMeta.TypeInfo);
            }
            catch (InvalidOperationException ex)
            {
                exceptionThrown = true;
                Assert.Contains("Id = 0", ex.Message);
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    #endregion

    #region Delete Tests

    [TestMethod]
    public void Delete_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "To Delete", Age = 30, Email = "delete@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            bool result = db.Delete<Person>(id, PersonMeta.TypeInfo);

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void Delete_ExistingDocument_RemovesFromDatabase()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "To Delete", Age = 30, Email = "delete@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            db.Delete<Person>(id, PersonMeta.TypeInfo);

            Person retrieved = db.GetById(id, PersonMeta.TypeInfo);

            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public void Delete_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            bool result = db.Delete<Person>(999, PersonMeta.TypeInfo);

            Assert.IsFalse(result);
        }
    }

    #endregion

    #region Full CRUD Cycle Tests

    [TestMethod]
    public void FullCRUDCycle_InsertGetUpdateDelete()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Full Cycle", Age = 25, Email = "cycle@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);
            Assert.AreEqual(1, id);

            Person retrieved = db.GetById(id, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Full Cycle", retrieved.Name);

            retrieved.Name = "Updated Cycle";
            retrieved.Age = 30;
            bool updated = db.Update(retrieved, PersonMeta.TypeInfo);
            Assert.IsTrue(updated);

            Person afterUpdate = db.GetById(id, PersonMeta.TypeInfo);
            Assert.AreEqual("Updated Cycle", afterUpdate.Name);
            Assert.AreEqual(30, afterUpdate.Age);

            bool deleted = db.Delete<Person>(id, PersonMeta.TypeInfo);
            Assert.IsTrue(deleted);

            Person afterDelete = db.GetById(id, PersonMeta.TypeInfo);
            Assert.IsNull(afterDelete);
        }
    }

    [TestMethod]
    public void FullCRUDCycle_PersistsAfterReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Persist Test", Age = 40, Email = "persist@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Persist Test", retrieved.Name);
            Assert.AreEqual(40, retrieved.Age);
        }
    }

    #endregion

    #region Query Tests

    [TestMethod]
    public void Query_ToList_ReturnsAllDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

            List<Person> results = db.Query(PersonMeta.TypeInfo).ToList();

            Assert.HasCount(3, results);
        }
    }

    [TestMethod]
    public void Query_WithWhereFilter_ReturnsFilteredDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

            List<Person> results = db.Query(PersonMeta.TypeInfo)
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 28)
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.TrueForAll(p => p.Age > 28));
        }
    }

    [TestMethod]
    public void Query_WithSkipAndLimit_ReturnsPaginatedResults()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            for (int i = 1; i <= 10; i++)
            {
                db.Insert(new Person { Name = $"Person {i}", Age = 20 + i, Email = $"person{i}@example.com" }, PersonMeta.TypeInfo);
            }

            List<Person> results = db.Query(PersonMeta.TypeInfo)
                .Skip(3)
                .Limit(4)
                .ToList();

            Assert.HasCount(4, results);
        }
    }

    [TestMethod]
    public void Query_FirstOrDefault_ReturnsFirstMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);

            Person result = db.Query(PersonMeta.TypeInfo)
                .Where(PersonMeta.Name, FieldOp.Equals, "Bob")
                .FirstOrDefault();

            Assert.IsNotNull(result);
            Assert.AreEqual("Bob", result.Name);
        }
    }

    [TestMethod]
    public void Query_FirstOrDefault_NoMatch_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);

            Person result = db.Query(PersonMeta.TypeInfo)
                .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent")
                .FirstOrDefault();

            Assert.IsNull(result);
        }
    }

    [TestMethod]
    public void Query_Count_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            for (int i = 1; i <= 5; i++)
            {
                db.Insert(new Person { Name = $"Person {i}", Age = 20 + i, Email = $"person{i}@example.com" }, PersonMeta.TypeInfo);
            }

            int count = db.Query(PersonMeta.TypeInfo).Count();

            Assert.AreEqual(5, count);
        }
    }

    [TestMethod]
    public void Query_CountWithFilter_ReturnsFilteredCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

            int count = db.Query(PersonMeta.TypeInfo)
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .Count();

            Assert.AreEqual(2, count);
        }
    }

    [TestMethod]
    public void Query_EmptyCollection_ReturnsEmptyList()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            List<Person> results = db.Query(PersonMeta.TypeInfo).ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_WhereBetween_ReturnsDocumentsInRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 20, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 30, Email = "charlie@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Diana", Age = 35, Email = "diana@example.com" }, PersonMeta.TypeInfo);

            List<Person> results = db.Query(PersonMeta.TypeInfo)
                .WhereBetween(PersonMeta.Age, 24, 31)
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.TrueForAll(p => p.Age >= 24 && p.Age <= 31));
        }
    }

    [TestMethod]
    public void Query_WhereIn_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

            List<Person> results = db.Query(PersonMeta.TypeInfo)
                .WhereIn(PersonMeta.Name, new string[] { "Alice", "Charlie" })
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.Exists(p => p.Name == "Alice"));
            Assert.IsTrue(results.Exists(p => p.Name == "Charlie"));
        }
    }

    #endregion

    #region GetAllDocuments Tests

    [TestMethod]
    public void GetAllDocuments_ReturnsAllDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

            List<Person> all = db.GetAllDocuments(PersonMeta.TypeInfo);

            Assert.HasCount(3, all);
        }
    }

    [TestMethod]
    public void GetAllDocuments_EmptyCollection_ReturnsEmptyList()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            List<Person> all = db.GetAllDocuments(PersonMeta.TypeInfo);

            Assert.IsEmpty(all);
        }
    }

    [TestMethod]
    public void GetAllDocuments_NonExistentCollection_ReturnsEmptyList()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            List<Person> all = db.GetAllDocuments(PersonMeta.TypeInfo);

            Assert.IsEmpty(all);
        }
    }

    #endregion
}
