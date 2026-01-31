using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using GaldrDatabase = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class ConflictDetectionTests
{
    private string _testDbPath;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"conflict_test_{Guid.NewGuid()}.gdb");
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }

        string walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }
    }

    [TestMethod]
    public void Transaction_Update_ConcurrentModification_ThrowsWriteConflict()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document
            Person person = new Person { Name = "Alice", Age = 30 };
            int id = db.Insert(person);

            // Start transaction 1 (gets snapshot)
            ITransaction tx1 = db.BeginTransaction();

            // Transaction 2 updates the document and commits
            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Alice Updated", Age = 31 };
                tx2.Replace(updated);
                tx2.Commit();
            }

            // Transaction 1 tries to update the same document - should conflict
            Person tx1Update = new Person { Id = id, Name = "Alice from tx1", Age = 32 };

            Assert.ThrowsExactly<WriteConflictException>(() =>
            {
                tx1.Replace(tx1Update);
            });

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Delete_ConcurrentModification_ThrowsWriteConflict()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document
            Person person = new Person { Name = "Bob", Age = 25 };
            int id = db.Insert(person);

            // Start transaction 1 (gets snapshot)
            ITransaction tx1 = db.BeginTransaction();

            // Transaction 2 updates the document and commits
            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Bob Updated", Age = 26 };
                tx2.Replace(updated);
                tx2.Commit();
            }

            // Transaction 1 tries to delete the same document - should conflict
            Assert.ThrowsExactly<WriteConflictException>(() =>
            {
                tx1.DeleteById<Person>(id);
            });

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Update_NoConflict_WhenNoOtherTransaction()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document
            Person person = new Person { Name = "Charlie", Age = 35 };
            int id = db.Insert(person);

            // Single transaction updates - no conflict
            using (ITransaction tx = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Charlie Updated", Age = 36 };
                bool result = tx.Replace(updated);
                tx.Commit();

                Assert.IsTrue(result);
            }

            // Verify the update
            Person retrieved = db.GetById<Person>(id);
            Assert.AreEqual("Charlie Updated", retrieved.Name);
            Assert.AreEqual(36, retrieved.Age);
        }
    }

    [TestMethod]
    public void Transaction_Commit_ValidatesWriteSet()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document
            Person person = new Person { Name = "Diana", Age = 40 };
            int id = db.Insert(person);

            // Start transaction 1 and buffer an update (doesn't check yet)
            ITransaction tx1 = db.BeginTransaction();
            Person tx1Update = new Person { Id = id, Name = "Diana from tx1", Age = 41 };
            tx1.Replace(tx1Update);

            // Transaction 2 updates and commits (after tx1 buffered but before tx1 commits)
            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person tx2Update = new Person { Id = id, Name = "Diana from tx2", Age = 42 };
                tx2.Replace(tx2Update);
                tx2.Commit();
            }

            // Transaction 1 commit should fail due to conflict detection at commit time
            Assert.ThrowsExactly<WriteConflictException>(() =>
            {
                tx1.Commit();
            });

            tx1.Dispose();

            // Verify tx2's changes persisted
            Person retrieved = db.GetById<Person>(id);
            Assert.AreEqual("Diana from tx2", retrieved.Name);
            Assert.AreEqual(42, retrieved.Age);
        }
    }

    [TestMethod]
    public void Transaction_Insert_WithExplicitId_ConflictsWithExisting()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document with explicit ID
            Person person = new Person { Id = 100, Name = "Eve", Age = 28 };
            db.Insert(person);

            // Try to insert another document with same ID - should conflict
            using (ITransaction tx = db.BeginTransaction())
            {
                Person duplicate = new Person { Id = 100, Name = "Eve Duplicate", Age = 29 };

                Assert.ThrowsExactly<WriteConflictException>(() =>
                {
                    tx.Insert(duplicate);
                });
            }
        }
    }

    [TestMethod]
    public void Transaction_SequentialUpdates_NoConflict()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert initial document
            Person person = new Person { Name = "Frank", Age = 50 };
            int id = db.Insert(person);

            // Sequential transactions should not conflict
            using (ITransaction tx1 = db.BeginTransaction())
            {
                Person update1 = new Person { Id = id, Name = "Frank v1", Age = 51 };
                tx1.Replace(update1);
                tx1.Commit();
            }

            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person update2 = new Person { Id = id, Name = "Frank v2", Age = 52 };
                tx2.Replace(update2);
                tx2.Commit();
            }

            Person retrieved = db.GetById<Person>(id);
            Assert.AreEqual("Frank v2", retrieved.Name);
            Assert.AreEqual(52, retrieved.Age);
        }
    }

    [TestMethod]
    public void Transaction_DifferentDocuments_NoConflict()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            // Insert two documents
            Person person1 = new Person { Name = "Grace", Age = 30 };
            Person person2 = new Person { Name = "Henry", Age = 35 };
            int id1 = db.Insert(person1);
            int id2 = db.Insert(person2);

            // Concurrent transactions on different documents should not conflict
            ITransaction tx1 = db.BeginTransaction();
            ITransaction tx2 = db.BeginTransaction();

            Person update1 = new Person { Id = id1, Name = "Grace Updated", Age = 31 };
            Person update2 = new Person { Id = id2, Name = "Henry Updated", Age = 36 };

            tx1.Replace(update1);
            tx2.Replace(update2);

            tx1.Commit();
            tx2.Commit();

            tx1.Dispose();
            tx2.Dispose();

            Person retrieved1 = db.GetById<Person>(id1);
            Person retrieved2 = db.GetById<Person>(id2);

            Assert.AreEqual("Grace Updated", retrieved1.Name);
            Assert.AreEqual("Henry Updated", retrieved2.Name);
        }
    }

    [TestMethod]
    public void WriteConflictException_ContainsConflictInfo()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Ivan", Age = 45 };
            int id = db.Insert(person);

            ITransaction tx1 = db.BeginTransaction();

            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Ivan Updated", Age = 46 };
                tx2.Replace(updated);
                tx2.Commit();
            }

            try
            {
                Person tx1Update = new Person { Id = id, Name = "Ivan from tx1", Age = 47 };
                tx1.Replace(tx1Update);
                Assert.Fail("Expected WriteConflictException");
            }
            catch (WriteConflictException ex)
            {
                Assert.AreEqual("Person", ex.CollectionName);
                Assert.AreEqual(id, ex.DocumentId);
                Assert.IsGreaterThan(0ul, ex.ConflictingTxId.Value);
            }
            finally
            {
                tx1.Dispose();
            }
        }
    }

    [TestMethod]
    public void Transaction_Rollback_DoesNotAffectVersionIndex()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Julia", Age = 33 };
            int id = db.Insert(person);

            // Start transaction and buffer changes but rollback
            using (ITransaction tx = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Julia Updated", Age = 34 };
                tx.Replace(updated);
                tx.Rollback();
            }

            // Another transaction should succeed without conflict
            using (ITransaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Julia v2", Age = 35 };
                tx2.Replace(updated);
                tx2.Commit();
            }

            Person retrieved = db.GetById<Person>(id);
            Assert.AreEqual("Julia v2", retrieved.Name);
        }
    }
}
