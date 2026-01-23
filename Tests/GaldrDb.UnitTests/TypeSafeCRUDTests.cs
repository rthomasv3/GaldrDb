using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
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
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person);

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
            LegacyCustomer customer = new LegacyCustomer { Name = "Test Customer", Email = "customer@example.com" };
            int id = db.Insert(customer);

            LegacyCustomer retrieved = db.GetById<LegacyCustomer>(id);

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
            Person person = new Person { Id = 0, Name = "Auto ID", Age = 30, Email = "auto@example.com" };
            int assignedId = db.Insert(person);

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
            Person person = new Person { Id = 100, Name = "Explicit ID", Age = 25, Email = "explicit@example.com" };
            int assignedId = db.Insert(person);

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
            Person person1 = new Person { Name = "First", Age = 20, Email = "first@example.com" };
            Person person2 = new Person { Name = "Second", Age = 25, Email = "second@example.com" };
            Person person3 = new Person { Name = "Third", Age = 30, Email = "third@example.com" };

            int id1 = db.Insert(person1);
            int id2 = db.Insert(person2);
            int id3 = db.Insert(person3);

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
            Person person1 = new Person { Id = 50, Name = "High ID", Age = 30, Email = "high@example.com" };
            db.Insert(person1);

            Person person2 = new Person { Name = "Auto After High", Age = 25, Email = "auto@example.com" };
            int id2 = db.Insert(person2);

            Assert.AreEqual(51, id2);
        }
    }

    [TestMethod]
    public void Insert_WithoutEnsureCollection_AutoCreatesCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert without calling EnsureCollection first - should auto-create
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person);

            Assert.AreEqual(1, id);

            Person retrieved = db.GetById<Person>(id);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }
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
            Person person = new Person { Name = "John Doe", Age = 35, Email = "john@example.com" };
            int id = db.Insert(person);

            Person retrieved = db.GetById<Person>(id);

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
            Person retrieved = db.GetById<Person>(999);

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
            Person person = new Person { Name = "Original", Age = 30, Email = "original@example.com" };
            int id = db.Insert(person);

            person.Name = "Updated";
            person.Age = 35;
            bool result = db.Replace(person);

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
            Person person = new Person { Name = "Original", Age = 30, Email = "original@example.com" };
            int id = db.Insert(person);

            person.Name = "Updated";
            person.Age = 35;
            person.Email = "updated@example.com";
            db.Replace(person);

            Person retrieved = db.GetById<Person>(id);

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
            Person person = new Person { Id = 999, Name = "Ghost", Age = 0, Email = "ghost@example.com" };
            bool result = db.Replace(person);

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
            try
            {
                Person person = new Person { Id = 0, Name = "Zero ID", Age = 25, Email = "zero@example.com" };
                db.Replace(person);
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
            Person person = new Person { Name = "To Delete", Age = 30, Email = "delete@example.com" };
            int id = db.Insert(person);

            bool result = db.DeleteById<Person>(id);

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
            Person person = new Person { Name = "To Delete", Age = 30, Email = "delete@example.com" };
            int id = db.Insert(person);

            db.DeleteById<Person>(id);

            Person retrieved = db.GetById<Person>(id);

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
            bool result = db.DeleteById<Person>(999);

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
            Person person = new Person { Name = "Full Cycle", Age = 25, Email = "cycle@example.com" };
            int id = db.Insert(person);
            Assert.AreEqual(1, id);

            Person retrieved = db.GetById<Person>(id);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Full Cycle", retrieved.Name);

            retrieved.Name = "Updated Cycle";
            retrieved.Age = 30;
            bool updated = db.Replace<Person>(retrieved);
            Assert.IsTrue(updated);

            Person afterUpdate = db.GetById<Person>(id);
            Assert.AreEqual("Updated Cycle", afterUpdate.Name);
            Assert.AreEqual(30, afterUpdate.Age);

            bool deleted = db.DeleteById<Person>(id);
            Assert.IsTrue(deleted);

            Person afterDelete = db.GetById<Person>(id);
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
            Person person = new Person { Name = "Persist Test", Age = 40, Email = "persist@example.com" };
            insertedId = db.Insert(person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved = db.GetById<Person>(insertedId);

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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            List<Person> results = db.Query<Person>().ToList();

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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            List<Person> results = db.Query<Person>()
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
            for (int i = 1; i <= 10; i++)
            {
                db.Insert(new Person { Name = $"Person {i}", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            List<Person> results = db.Query<Person>()
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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            Person result = db.Query<Person>()
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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            Person result = db.Query<Person>()
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
            for (int i = 1; i <= 5; i++)
            {
                db.Insert(new Person { Name = $"Person {i}", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            int count = db.Query<Person>().Count();

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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            int count = db.Query<Person>()
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
            List<Person> results = db.Query<Person>().ToList();

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
            db.Insert(new Person { Name = "Alice", Age = 20, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 30, Email = "charlie@example.com" });
            db.Insert(new Person { Name = "Diana", Age = 35, Email = "diana@example.com" });

            List<Person> results = db.Query<Person>()
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
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            List<Person> results = db.Query<Person>()
                .WhereIn(PersonMeta.Name, new string[] { "Alice", "Charlie" })
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.Exists(p => p.Name == "Alice"));
            Assert.IsTrue(results.Exists(p => p.Name == "Charlie"));
        }
    }

    #endregion

    #region Parameter-less API Tests (Auto-resolved TypeInfo)

    [TestMethod]
    public void ParameterlessApi_FullCrudCycle_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // NO EnsureCollection call - Insert should auto-ensure

            // Insert without typeInfo parameter
            Person person = new Person { Name = "Auto API Test", Age = 42, Email = "auto@test.com" };
            int id = db.Insert(person);
            Assert.AreEqual(1, id);

            // GetById without typeInfo parameter
            Person retrieved = db.GetById<Person>(id);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Auto API Test", retrieved.Name);
            Assert.AreEqual(42, retrieved.Age);

            // Update without typeInfo parameter
            retrieved.Age = 43;
            bool updated = db.Replace(retrieved);
            Assert.IsTrue(updated);

            Person afterUpdate = db.GetById<Person>(id);
            Assert.AreEqual(43, afterUpdate.Age);

            // Query without typeInfo parameter
            List<Person> queryResults = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.Equals, 43)
                .ToList();
            Assert.HasCount(1, queryResults);

            // Query all documents without typeInfo parameter
            List<Person> all = db.Query<Person>().ToList();
            Assert.HasCount(1, all);

            // Delete without typeInfo parameter
            bool deleted = db.DeleteById<Person>(id);
            Assert.IsTrue(deleted);

            Person afterDelete = db.GetById<Person>(id);
            Assert.IsNull(afterDelete);
        }
    }

    [TestMethod]
    public void ParameterlessApi_MixedWithExplicitTypeInfo_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // NO EnsureCollection call - first Insert should auto-ensure

            // Insert with explicit typeInfo
            Person person1 = new Person { Name = "Explicit", Age = 30, Email = "explicit@test.com" };
            int id1 = db.Insert(person1);

            // Insert without typeInfo
            Person person2 = new Person { Name = "Implicit", Age = 35, Email = "implicit@test.com" };
            int id2 = db.Insert(person2);

            // GetById mixing both styles
            Person retrieved1 = db.GetById<Person>(id1);
            Person retrieved2 = db.GetById<Person>(id2);

            Assert.AreEqual("Explicit", retrieved1.Name);
            Assert.AreEqual("Implicit", retrieved2.Name);

            // Query without typeInfo
            List<Person> all = db.Query<Person>().ToList();
            Assert.HasCount(2, all);
        }
    }

    [TestMethod]
    public void AutoEnsureCollection_InsertCreatesCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert directly without EnsureCollection - should auto-create
            Person person = new Person { Name = "Auto Created", Age = 25, Email = "auto@test.com" };
            int id = db.Insert(person);

            Assert.AreEqual(1, id);

            Person retrieved = db.GetById<Person>(id);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Auto Created", retrieved.Name);
        }
    }

    [TestMethod]
    public void AutoEnsureCollection_UpdateCreatesCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Update on non-existent collection should auto-create (but return false since doc doesn't exist)
            Person person = new Person { Id = 999, Name = "Update Test", Age = 30, Email = "update@test.com" };
            bool updated = db.Replace(person);

            Assert.IsFalse(updated);

            // Now insert should work since collection was created
            Person newPerson = new Person { Name = "After Update", Age = 35, Email = "after@test.com" };
            int id = db.Insert(newPerson);
            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void AutoEnsureCollection_DeleteCreatesCollection()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Delete on non-existent collection should auto-create (but return false since doc doesn't exist)
            bool deleted = db.DeleteById<Person>(999);

            Assert.IsFalse(deleted);

            // Now insert should work since collection was created
            Person person = new Person { Name = "After Delete", Age = 40, Email = "afterdel@test.com" };
            int id = db.Insert(person);
            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void AutoEnsureCollection_WithIndexedFields_CreatesIndexes()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert directly - should auto-create collection AND indexes
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@test.com" });
            db.Insert(new Person { Name = "Charlie", Age = 25, Email = "charlie@test.com" });

            // Query using indexed field - should use the auto-created index
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Bob")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Bob", results[0].Name);
        }
    }

    #endregion

    #region Optimized Count Tests

    [TestMethod]
    public void Count_UnfilteredWithWriteSetInserts_IncludesUncommittedInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
                tx.Insert(new Person { Name = "Diana", Age = 28, Email = "diana@example.com" });

                int count = tx.Query<Person>().Count();

                Assert.AreEqual(4, count);
            }
        }
    }

    [TestMethod]
    public void Count_UnfilteredWithWriteSetDeletes_ExcludesDeletedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(1);

                int count = tx.Query<Person>().Count();

                Assert.AreEqual(2, count);
            }
        }
    }

    [TestMethod]
    public void Count_FilteredWithWriteSetInserts_IncludesMatchingUncommittedInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
                tx.Insert(new Person { Name = "Diana", Age = 22, Email = "diana@example.com" });

                int count = tx.Query<Person>()
                    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                    .Count();

                Assert.AreEqual(2, count);
            }
        }
    }

    [TestMethod]
    public void Count_FilteredWithWriteSetDeletes_ExcludesDeletedMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(2);

                int count = tx.Query<Person>()
                    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                    .Count();

                Assert.AreEqual(1, count);
            }
        }
    }

    [TestMethod]
    public void Count_FilteredWithWriteSetUpdates_ReflectsUpdatedValues()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Replace(new Person { Id = 1, Name = "Alice", Age = 40, Email = "alice@example.com" });

                int count = tx.Query<Person>()
                    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                    .Count();

                Assert.AreEqual(3, count);
            }
        }
    }

    [TestMethod]
    public void Count_FilteredWithWriteSetUpdates_ExcludesNoLongerMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Replace(new Person { Id = 2, Name = "Bob", Age = 20, Email = "bob@example.com" });

                int count = tx.Query<Person>()
                    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                    .Count();

                Assert.AreEqual(1, count);
            }
        }
    }

    [TestMethod]
    public void Count_EmptyCollection_ReturnsZero()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            int count = db.Query<Person>().Count();

            Assert.AreEqual(0, count);
        }
    }

    [TestMethod]
    public void Count_FilteredNoMatches_ReturnsZero()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            int count = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 100)
                .Count();

            Assert.AreEqual(0, count);
        }
    }

    [TestMethod]
    public async Task CountAsync_Unfiltered_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertAsync(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            await db.InsertAsync(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            await db.InsertAsync(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            int count = await db.Query<Person>().CountAsync();

            Assert.AreEqual(3, count);
        }
    }

    [TestMethod]
    public async Task CountAsync_Filtered_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertAsync(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            await db.InsertAsync(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            await db.InsertAsync(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            int count = await db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .CountAsync();

            Assert.AreEqual(2, count);
        }
    }

    [TestMethod]
    public void Count_WithIdFilter_UsesOptimizedPrimaryKeyPath()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 1; i <= 10; i++)
            {
                db.Insert(new Person { Name = $"Person {i}", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            int count = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThanOrEqual, 5)
                .Where(PersonMeta.Id, FieldOp.LessThanOrEqual, 8)
                .Count();

            Assert.AreEqual(4, count);
        }
    }

    #endregion
}
