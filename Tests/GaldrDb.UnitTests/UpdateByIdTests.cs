using System;
using System.IO;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class UpdateByIdTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbUpdateByIdTests_{Guid.NewGuid()}");
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

    #region Basic Partial Update Tests

    [TestMethod]
    public void UpdateById_SingleField_UpdatesField()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Alice Smith")
                    .Execute();

                Assert.IsTrue(result);
                tx.Commit();
            }

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Alice Smith", updated.Name);
            Assert.AreEqual(30, updated.Age);
            Assert.AreEqual("alice@example.com", updated.Email);
        }
    }

    [TestMethod]
    public void UpdateById_MultipleFields_UpdatesAllFields()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Robert")
                    .Set(PersonMeta.Age, 26)
                    .Set(PersonMeta.Email, "robert@example.com")
                    .Execute();

                Assert.IsTrue(result);
                tx.Commit();
            }

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Robert", updated.Name);
            Assert.AreEqual(26, updated.Age);
            Assert.AreEqual("robert@example.com", updated.Email);
        }
    }

    [TestMethod]
    public void UpdateById_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = tx.UpdateById<Person>(999)
                    .Set(PersonMeta.Name, "NonExistent")
                    .Execute();

                Assert.IsFalse(result);
            }
        }
    }

    [TestMethod]
    public void UpdateById_WithNullValue_SetsFieldToNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Email, null)
                    .Execute();

                Assert.IsTrue(result);
                tx.Commit();
            }

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Charlie", updated.Name);
            Assert.IsNull(updated.Email);
        }
    }

    #endregion

    #region Transaction Rollback Tests

    [TestMethod]
    public void UpdateById_TransactionRollback_RevertsChanges()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "David", Age = 40, Email = "david@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Dave")
                    .Execute();

                // Don't commit - let transaction dispose (implicit rollback)
            }

            Person unchanged = db.GetById<Person>(id);
            Assert.AreEqual("David", unchanged.Name);
        }
    }

    [TestMethod]
    public void UpdateById_ExplicitRollback_RevertsChanges()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Eve", Age = 28, Email = "eve@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Age, 29)
                    .Execute();

                tx.Rollback();
            }

            Person unchanged = db.GetById<Person>(id);
            Assert.AreEqual(28, unchanged.Age);
        }
    }

    #endregion

    #region GaldrDb Auto-Commit Tests

    [TestMethod]
    public void UpdateById_GaldrDbPath_AutoCommits()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Frank", Age = 45, Email = "frank@example.com" });

            bool result = db.UpdateById<Person>(id)
                .Set(PersonMeta.Name, "Franklin")
                .Execute();

            Assert.IsTrue(result);

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Franklin", updated.Name);
        }
    }

    [TestMethod]
    public void UpdateById_GaldrDbPath_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            bool result = db.UpdateById<Person>(999)
                .Set(PersonMeta.Name, "NonExistent")
                .Execute();

            Assert.IsFalse(result);
        }
    }

    #endregion

    #region Read-Your-Own-Writes Tests

    [TestMethod]
    public void UpdateById_AfterInsertInSameTransaction_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                int id = tx.Insert(new Person { Name = "Grace", Age = 32, Email = "grace@example.com" });

                bool result = tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Grace Hopper")
                    .Execute();

                Assert.IsTrue(result);
                tx.Commit();
            }

            Person updated = db.GetById<Person>(1);
            Assert.AreEqual("Grace Hopper", updated.Name);
        }
    }

    [TestMethod]
    public void UpdateById_AfterPreviousUpdateById_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Henry", Age = 50, Email = "henry@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Hank")
                    .Execute();

                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Age, 51)
                    .Execute();

                tx.Commit();
            }

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Hank", updated.Name);
            Assert.AreEqual(51, updated.Age);
        }
    }

    #endregion

    #region Async Tests

    [TestMethod]
    public async Task UpdateByIdAsync_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Ivy", Age = 27, Email = "ivy@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = await tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Ivy League")
                    .ExecuteAsync();

                Assert.IsTrue(result);
                await tx.CommitAsync();
            }

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Ivy League", updated.Name);
        }
    }

    [TestMethod]
    public async Task UpdateByIdAsync_GaldrDbPath_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Jack", Age = 33, Email = "jack@example.com" });

            bool result = await db.UpdateById<Person>(id)
                .Set(PersonMeta.Age, 34)
                .ExecuteAsync();

            Assert.IsTrue(result);

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual(34, updated.Age);
        }
    }

    #endregion

    #region Index Maintenance Tests

    [TestMethod]
    public void UpdateById_IndexedField_UpdatesIndex()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Kate", Age = 29, Email = "kate@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Name, "Katherine")
                    .Execute();

                tx.Commit();
            }

            // Query by old name should return nothing
            Person oldResult = db.Query<Person>()
                .Where(PersonMeta.Name, GaldrDbEngine.Query.FieldOp.Equals, "Kate")
                .FirstOrDefault();
            Assert.IsNull(oldResult);

            // Query by new name should return the document
            Person newResult = db.Query<Person>()
                .Where(PersonMeta.Name, GaldrDbEngine.Query.FieldOp.Equals, "Katherine")
                .FirstOrDefault();
            Assert.IsNotNull(newResult);
            Assert.AreEqual(id, newResult.Id);
        }
    }

    [TestMethod]
    public void UpdateById_NonIndexedField_DoesNotAffectIndexQueries()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Leo", Age = 36, Email = "leo@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                // Age is not indexed
                tx.UpdateById<Person>(id)
                    .Set(PersonMeta.Age, 37)
                    .Execute();

                tx.Commit();
            }

            // Query by name (indexed) should still work
            Person result = db.Query<Person>()
                .Where(PersonMeta.Name, GaldrDbEngine.Query.FieldOp.Equals, "Leo")
                .FirstOrDefault();
            Assert.IsNotNull(result);
            Assert.AreEqual(37, result.Age);
        }
    }

    #endregion

    #region Conflict Detection Tests

    [TestMethod]
    public void UpdateById_ConcurrentModification_ThrowsWriteConflict()
    {
        string dbPath = Path.Combine(_testDirectory, "test.galdrdb");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { PageSize = 8192, UseWal = true }))
        {
            int id = db.Insert(new Person { Name = "Mike", Age = 42, Email = "mike@example.com" });

            using (ITransaction tx1 = db.BeginTransaction())
            {
                // tx1 starts
                Person p1 = tx1.GetById<Person>(id);

                // Another transaction modifies the document
                using (ITransaction tx2 = db.BeginTransaction())
                {
                    tx2.UpdateById<Person>(id)
                        .Set(PersonMeta.Name, "Michael")
                        .Execute();
                    tx2.Commit();
                }

                // tx1 tries to update - should throw
                Assert.Throws<WriteConflictException>(() =>
                {
                    tx1.UpdateById<Person>(id)
                        .Set(PersonMeta.Age, 43)
                        .Execute();
                });
            }
        }
    }

    #endregion
}
