using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using GaldrDatabase = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class NoWalIntegrationTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"nowal_test_{Guid.NewGuid()}");
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

    private GaldrDbOptions NoWalOptions() => new GaldrDbOptions { PageSize = 8192, UseWal = false };

    [TestMethod]
    public void BasicInsertAndQuery_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            int id = db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
            Assert.IsTrue(id > 0);

            Person result = db.GetById<Person>(id);
            Assert.IsNotNull(result);
            Assert.AreEqual("Alice", result.Name);
            Assert.AreEqual(30, result.Age);
        }
    }

    [TestMethod]
    public void MultipleInserts_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@test.com" });
            }

            List<Person> all = db.Query<Person>().ToList();
            Assert.AreEqual(50, all.Count);
        }
    }

    [TestMethod]
    public void InsertUpdateDelete_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            // Insert
            int id = db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@test.com" });

            // Update
            db.Replace(new Person { Id = id, Name = "Bob Updated", Age = 26, Email = "bob2@test.com" });

            Person updated = db.GetById<Person>(id);
            Assert.AreEqual("Bob Updated", updated.Name);
            Assert.AreEqual(26, updated.Age);

            // Delete
            db.DeleteById<Person>(id);
            Person deleted = db.GetById<Person>(id);
            Assert.IsNull(deleted);
        }
    }

    [TestMethod]
    public void TransactionCommit_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "TxPerson", Age = 35, Email = "tx@test.com" });
                tx.Commit();
            }

            List<Person> results = db.Query<Person>().ToList();
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("TxPerson", results[0].Name);
        }
    }

    [TestMethod]
    public void TransactionRollback_DiscardsWrites_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            // Insert one doc first
            db.Insert(new Person { Name = "Existing", Age = 40, Email = "exist@test.com" });

            // Start transaction, insert, then dispose without commit (rollback)
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Uncommitted", Age = 99, Email = "none@test.com" });
                // No Commit() — implicit rollback on dispose
            }

            List<Person> results = db.Query<Person>().ToList();
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("Existing", results[0].Name);
        }
    }

    [TestMethod]
    public void ConcurrentTransactions_ConflictDetection_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 20, Email = "orig@test.com" });

            // Tx1 starts and reads
            ITransaction tx1 = db.BeginTransaction();

            // Tx2 updates and commits
            using (ITransaction tx2 = db.BeginTransaction())
            {
                tx2.Replace(new Person { Id = id, Name = "From Tx2", Age = 21, Email = "tx2@test.com" });
                tx2.Commit();
            }

            // Tx1 tries to update the same doc — should conflict at MVCC level
            Assert.ThrowsExactly<WriteConflictException>(() =>
            {
                tx1.Replace(new Person { Id = id, Name = "From Tx1", Age = 22, Email = "tx1@test.com" });
            });

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void PersistenceAcrossOpenClose_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        // Create and insert
        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            db.Insert(new Person { Name = "Persistent", Age = 50, Email = "persist@test.com" });
        }

        // Reopen and verify
        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, NoWalOptions()))
        {
            List<Person> results = db.Query<Person>().ToList();
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("Persistent", results[0].Name);
        }
    }

    [TestMethod]
    public void Checkpoint_NoWal_ReturnsSilently()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@test.com" });

            // Checkpoint should be a no-op but not throw
            db.Checkpoint();
        }
    }

    [TestMethod]
    public void MultipleTransactionsSequential_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            for (int i = 0; i < 10; i++)
            {
                using (ITransaction tx = db.BeginTransaction())
                {
                    tx.Insert(new Person { Name = $"TxPerson{i}", Age = 20 + i, Email = $"tx{i}@test.com" });
                    tx.Commit();
                }
            }

            List<Person> results = db.Query<Person>().ToList();
            Assert.AreEqual(10, results.Count);
        }
    }

    [TestMethod]
    public void DropCollection_NoWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, NoWalOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@test.com" });
            db.Insert(new Person { Name = "Test2", Age = 30, Email = "test2@test.com" });

            db.DropCollection("Person", deleteDocuments: true);

            Assert.IsFalse(db.GetCollectionNames().Contains("Person"));
        }
    }
}
