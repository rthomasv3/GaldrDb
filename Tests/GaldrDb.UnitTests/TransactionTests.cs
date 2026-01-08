using System;
using System.Collections.Generic;
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
public class TransactionTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbTransactionTests_{Guid.NewGuid()}");
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

    #region TxId Tests

    [TestMethod]
    public void TxId_None_IsNotValid()
    {
        TxId txId = TxId.None;

        Assert.IsFalse(txId.IsValid);
        Assert.AreEqual(0ul, txId.Value);
    }

    [TestMethod]
    public void TxId_ValidId_IsValid()
    {
        TxId txId = new TxId(1);

        Assert.IsTrue(txId.IsValid);
        Assert.AreEqual(1ul, txId.Value);
    }

    [TestMethod]
    public void TxId_Comparison_WorksCorrectly()
    {
        TxId tx1 = new TxId(1);
        TxId tx2 = new TxId(2);
        TxId tx1Copy = new TxId(1);

        Assert.IsTrue(tx1 < tx2);
        Assert.IsTrue(tx2 > tx1);
        Assert.IsTrue(tx1 <= tx2);
        Assert.IsTrue(tx1 <= tx1Copy);
        Assert.IsTrue(tx1 == tx1Copy);
        Assert.IsTrue(tx1 != tx2);
    }

    [TestMethod]
    public void TxId_Increment_WorksCorrectly()
    {
        TxId txId = new TxId(5);
        txId++;

        Assert.AreEqual(6ul, txId.Value);
    }

    #endregion

    #region TransactionManager Tests

    [TestMethod]
    public void TransactionManager_AllocateTxId_IncrementsSequentially()
    {
        TransactionManager txManager = new TransactionManager();

        TxId tx1 = txManager.AllocateTxId();
        TxId tx2 = txManager.AllocateTxId();
        TxId tx3 = txManager.AllocateTxId();

        Assert.AreEqual(1ul, tx1.Value);
        Assert.AreEqual(2ul, tx2.Value);
        Assert.AreEqual(3ul, tx3.Value);
    }

    [TestMethod]
    public void TransactionManager_RegisterAndUnregister_TracksCorrectly()
    {
        TransactionManager txManager = new TransactionManager();
        TxId tx1 = txManager.AllocateTxId();
        TxId tx2 = txManager.AllocateTxId();
        TxId snapshot = txManager.GetSnapshotTxId();

        txManager.RegisterTransaction(tx1, snapshot);
        txManager.RegisterTransaction(tx2, snapshot);

        Assert.AreEqual(2, txManager.ActiveTransactionCount);
        Assert.IsTrue(txManager.IsTransactionActive(tx1));
        Assert.IsTrue(txManager.IsTransactionActive(tx2));

        txManager.UnregisterTransaction(tx1);

        Assert.AreEqual(1, txManager.ActiveTransactionCount);
        Assert.IsFalse(txManager.IsTransactionActive(tx1));
        Assert.IsTrue(txManager.IsTransactionActive(tx2));
    }

    [TestMethod]
    public void TransactionManager_MarkCommitted_UpdatesLastCommitted()
    {
        TransactionManager txManager = new TransactionManager();
        TxId tx1 = txManager.AllocateTxId();
        TxId tx2 = txManager.AllocateTxId();
        TxId snapshot = txManager.GetSnapshotTxId();

        txManager.RegisterTransaction(tx1, snapshot);
        txManager.RegisterTransaction(tx2, snapshot);

        Assert.AreEqual(TxId.None, txManager.LastCommittedTxId);

        txManager.MarkCommitted(tx1);

        Assert.AreEqual(tx1, txManager.LastCommittedTxId);
        Assert.IsFalse(txManager.IsTransactionActive(tx1));

        txManager.MarkCommitted(tx2);

        Assert.AreEqual(tx2, txManager.LastCommittedTxId);
    }

    [TestMethod]
    public void TransactionManager_GetOldestActiveTransaction_ReturnsCorrect()
    {
        TransactionManager txManager = new TransactionManager();
        TxId tx1 = txManager.AllocateTxId();
        TxId tx2 = txManager.AllocateTxId();
        TxId tx3 = txManager.AllocateTxId();
        TxId snapshot = txManager.GetSnapshotTxId();

        txManager.RegisterTransaction(tx2, snapshot);
        txManager.RegisterTransaction(tx3, snapshot);

        TxId oldest = txManager.GetOldestActiveTransaction();

        Assert.AreEqual(tx2, oldest);

        txManager.UnregisterTransaction(tx2);
        oldest = txManager.GetOldestActiveTransaction();

        Assert.AreEqual(tx3, oldest);
    }

    #endregion

    #region Transaction Lifecycle Tests

    [TestMethod]
    public void Transaction_BeginTransaction_CreatesActiveTransaction()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                Assert.IsTrue(tx.TxId.IsValid);
                Assert.AreEqual(TransactionState.Active, tx.State);
                Assert.IsFalse(tx.IsReadOnly);
            }
        }
    }

    [TestMethod]
    public void Transaction_BeginReadOnlyTransaction_CreatesReadOnlyTransaction()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginReadOnlyTransaction())
            {
                Assert.IsTrue(tx.TxId.IsValid);
                Assert.AreEqual(TransactionState.Active, tx.State);
                Assert.IsTrue(tx.IsReadOnly);
            }
        }
    }

    [TestMethod]
    public void Transaction_Commit_ChangesStateToCommitted()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Commit();

                Assert.AreEqual(TransactionState.Committed, tx.State);
            }
        }
    }

    [TestMethod]
    public void Transaction_Rollback_ChangesStateToAborted()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Rollback();

                Assert.AreEqual(TransactionState.Aborted, tx.State);
            }
        }
    }

    [TestMethod]
    public void Transaction_DisposeWithoutCommit_Rollsback()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };
        TransactionState stateAfterDispose = TransactionState.Active;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Transaction tx = db.BeginTransaction();
            tx.Dispose();
            stateAfterDispose = tx.State;
        }

        Assert.AreEqual(TransactionState.Aborted, stateAfterDispose);
    }

    #endregion

    #region Write Set Tests

    [TestMethod]
    public void Transaction_Insert_AddsToWriteSet()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
                int id = tx.Insert(person, PersonMeta.TypeInfo);

                Assert.AreEqual(1, tx.WriteSetCount);
                Assert.IsGreaterThan(0, id);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public void Transaction_Insert_ReadYourOwnWrites()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
                int id = tx.Insert(person, PersonMeta.TypeInfo);

                // Read within same transaction should see the write
                Person retrieved = tx.GetById<Person>(id, PersonMeta.TypeInfo);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Test", retrieved.Name);
                Assert.AreEqual(30, retrieved.Age);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public void Transaction_Rollback_DiscardsWriteSet()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
                insertedId = tx.Insert(person, PersonMeta.TypeInfo);

                tx.Rollback();
            }

            // Document should not exist after rollback
            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public void Transaction_Commit_PersistsWriteSet()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
                insertedId = tx.Insert(person, PersonMeta.TypeInfo);

                tx.Commit();
            }

            // Document should exist after commit
            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }
    }

    [TestMethod]
    public void Transaction_Update_ModifiesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert initial document
            Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            // Update in transaction
            using (Transaction tx = db.BeginTransaction())
            {
                Person updated = new Person { Id = insertedId, Name = "Updated", Age = 30, Email = "updated@example.com" };
                bool result = tx.Update(updated, PersonMeta.TypeInfo);

                Assert.IsTrue(result);

                tx.Commit();
            }

            // Verify update persisted
            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.AreEqual("Updated", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);
        }
    }

    [TestMethod]
    public void Transaction_Delete_RemovesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert initial document
            Person person = new Person { Name = "ToDelete", Age = 25, Email = "delete@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            // Delete in transaction
            using (Transaction tx = db.BeginTransaction())
            {
                bool result = tx.Delete<Person>(insertedId, PersonMeta.TypeInfo);

                Assert.IsTrue(result);

                // Read within transaction should return null
                Person withinTx = tx.GetById<Person>(insertedId, PersonMeta.TypeInfo);
                Assert.IsNull(withinTx);

                tx.Commit();
            }

            // Verify delete persisted
            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public void Transaction_MultipleOperations_AllCommittedTogether()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person1 = new Person { Name = "Person1", Age = 25, Email = "p1@example.com" };
                id1 = tx.Insert(person1, PersonMeta.TypeInfo);

                Person person2 = new Person { Name = "Person2", Age = 30, Email = "p2@example.com" };
                id2 = tx.Insert(person2, PersonMeta.TypeInfo);

                Person person3 = new Person { Name = "Person3", Age = 35, Email = "p3@example.com" };
                id3 = tx.Insert(person3, PersonMeta.TypeInfo);

                Assert.AreEqual(3, tx.WriteSetCount);

                tx.Commit();
            }

            // All should be persisted
            Person r1 = db.GetById<Person>(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById<Person>(id2, PersonMeta.TypeInfo);
            Person r3 = db.GetById<Person>(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
        }
    }

    #endregion

    #region Read-Only Transaction Tests

    [TestMethod]
    public void ReadOnlyTransaction_CannotInsert()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            bool exceptionThrown = false;

            using (Transaction tx = db.BeginReadOnlyTransaction())
            {
                try
                {
                    Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
                    tx.Insert(person, PersonMeta.TypeInfo);
                }
                catch (InvalidOperationException)
                {
                    exceptionThrown = true;
                }
            }

            Assert.IsTrue(exceptionThrown);
        }
    }

    [TestMethod]
    public void ReadOnlyTransaction_CanRead()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Readable", Age = 25, Email = "read@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginReadOnlyTransaction())
            {
                Person retrieved = tx.GetById<Person>(insertedId, PersonMeta.TypeInfo);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Readable", retrieved.Name);

                tx.Commit();
            }
        }
    }

    #endregion

    #region Query Tests

    [TestMethod]
    public void Transaction_Query_SeesUncommittedInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert some initial data
            Person existing = new Person { Name = "Existing", Age = 25, Email = "existing@example.com" };
            db.Insert(existing, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                // Insert within transaction
                Person newPerson = new Person { Name = "New", Age = 30, Email = "new@example.com" };
                tx.Insert(newPerson, PersonMeta.TypeInfo);

                // Query should see both
                List<Person> results = tx.Query<Person>(PersonMeta.TypeInfo).ToList();

                Assert.HasCount(2, results);

                tx.Rollback();
            }

            // After rollback, only original document remains
            List<Person> afterRollback = db.Query<Person>(PersonMeta.TypeInfo).ToList();
            Assert.HasCount(1, afterRollback);
        }
    }

    [TestMethod]
    public void Transaction_Query_SeesUncommittedUpdates()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                // Update within transaction
                Person updated = new Person { Id = id, Name = "Updated", Age = 30, Email = "updated@example.com" };
                tx.Update(updated, PersonMeta.TypeInfo);

                // Query should see updated version
                List<Person> results = tx.Query<Person>(PersonMeta.TypeInfo)
                    .Where(PersonMeta.Name, GaldrDbEngine.Query.FieldOp.Equals, "Updated")
                    .ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("Updated", results[0].Name);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public void Transaction_Query_DoesNotSeeDeletedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person1 = new Person { Name = "Keep", Age = 25, Email = "keep@example.com" };
            Person person2 = new Person { Name = "Delete", Age = 30, Email = "delete@example.com" };
            db.Insert(person1, PersonMeta.TypeInfo);
            int deleteId = db.Insert(person2, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                // Delete within transaction
                tx.Delete<Person>(deleteId, PersonMeta.TypeInfo);

                // Query should not see deleted document
                List<Person> results = tx.Query<Person>(PersonMeta.TypeInfo).ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("Keep", results[0].Name);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public void Transaction_Query_WithFilters_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" }, PersonMeta.TypeInfo);
                tx.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" }, PersonMeta.TypeInfo);
                tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" }, PersonMeta.TypeInfo);

                // Query with filter
                List<Person> results = tx.Query<Person>(PersonMeta.TypeInfo)
                    .Where(PersonMeta.Name, GaldrDbEngine.Query.FieldOp.Equals, "Bob")
                    .ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("Bob", results[0].Name);

                tx.Commit();
            }
        }
    }

    #endregion

    #region Transaction Persistence Tests

    [TestMethod]
    public void Transaction_CommittedData_SurvivesReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Persistent", Age = 40, Email = "persist@example.com" };
                insertedId = tx.Insert(person, PersonMeta.TypeInfo);

                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Persistent", retrieved.Name);
            Assert.AreEqual(40, retrieved.Age);
        }
    }

    #endregion

    #region Async CRUD Tests

    [TestMethod]
    public async Task TransactionAsync_InsertAsync_AddsToWriteSet()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "AsyncTest", Age = 30, Email = "async@example.com" };
                int id = await tx.InsertAsync(person, PersonMeta.TypeInfo);

                Assert.AreEqual(1, tx.WriteSetCount);
                Assert.IsGreaterThan(0, id);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public async Task TransactionAsync_InsertAsync_ReadYourOwnWrites()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "AsyncRead", Age = 25, Email = "asyncread@example.com" };
                int id = await tx.InsertAsync(person, PersonMeta.TypeInfo);

                Person retrieved = await tx.GetByIdAsync<Person>(id, PersonMeta.TypeInfo);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("AsyncRead", retrieved.Name);
                Assert.AreEqual(25, retrieved.Age);

                tx.Rollback();
            }
        }
    }

    [TestMethod]
    public async Task TransactionAsync_CommitAsync_PersistsWriteSet()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "AsyncCommit", Age = 35, Email = "asynccommit@example.com" };
                insertedId = await tx.InsertAsync(person, PersonMeta.TypeInfo);

                await tx.CommitAsync();
            }

            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("AsyncCommit", retrieved.Name);
        }
    }

    [TestMethod]
    public async Task TransactionAsync_UpdateAsync_ModifiesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "OriginalAsync", Age = 25, Email = "originalasync@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person updated = new Person { Id = insertedId, Name = "UpdatedAsync", Age = 30, Email = "updatedasync@example.com" };
                bool result = await tx.UpdateAsync(updated, PersonMeta.TypeInfo);

                Assert.IsTrue(result);

                await tx.CommitAsync();
            }

            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.AreEqual("UpdatedAsync", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);
        }
    }

    [TestMethod]
    public async Task TransactionAsync_DeleteAsync_RemovesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "ToDeleteAsync", Age = 25, Email = "deleteasync@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                bool result = await tx.DeleteAsync<Person>(insertedId, PersonMeta.TypeInfo);

                Assert.IsTrue(result);

                Person withinTx = await tx.GetByIdAsync<Person>(insertedId, PersonMeta.TypeInfo);
                Assert.IsNull(withinTx);

                await tx.CommitAsync();
            }

            Person retrieved = db.GetById<Person>(insertedId, PersonMeta.TypeInfo);
            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public async Task TransactionAsync_MultipleAsyncOperations_AllCommittedTogether()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person1 = new Person { Name = "AsyncPerson1", Age = 25, Email = "ap1@example.com" };
                id1 = await tx.InsertAsync(person1, PersonMeta.TypeInfo);

                Person person2 = new Person { Name = "AsyncPerson2", Age = 30, Email = "ap2@example.com" };
                id2 = await tx.InsertAsync(person2, PersonMeta.TypeInfo);

                Person person3 = new Person { Name = "AsyncPerson3", Age = 35, Email = "ap3@example.com" };
                id3 = await tx.InsertAsync(person3, PersonMeta.TypeInfo);

                Assert.AreEqual(3, tx.WriteSetCount);

                await tx.CommitAsync();
            }

            Person r1 = db.GetById<Person>(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById<Person>(id2, PersonMeta.TypeInfo);
            Person r3 = db.GetById<Person>(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
        }
    }

    #endregion

    #region GaldrDb Async API Tests

    [TestMethod]
    public async Task GaldrDbAsync_InsertAsync_InsertsDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "DbAsyncInsert", Age = 40, Email = "dbasync@example.com" };
            int id = await db.InsertAsync(person, PersonMeta.TypeInfo);

            Assert.IsGreaterThan(0, id);

            Person retrieved = db.GetById<Person>(id, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("DbAsyncInsert", retrieved.Name);
        }
    }

    [TestMethod]
    public async Task GaldrDbAsync_GetByIdAsync_RetrievesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "DbAsyncGet", Age = 45, Email = "dbasyncget@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Person retrieved = await db.GetByIdAsync<Person>(id, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("DbAsyncGet", retrieved.Name);
            Assert.AreEqual(45, retrieved.Age);
        }
    }

    [TestMethod]
    public async Task GaldrDbAsync_UpdateAsync_UpdatesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "DbAsyncUpdateOrig", Age = 50, Email = "dbasyncupdate@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Person updated = new Person { Id = id, Name = "DbAsyncUpdateNew", Age = 55, Email = "dbasyncupdatenew@example.com" };
            bool result = await db.UpdateAsync(updated, PersonMeta.TypeInfo);

            Assert.IsTrue(result);

            Person retrieved = db.GetById<Person>(id, PersonMeta.TypeInfo);
            Assert.AreEqual("DbAsyncUpdateNew", retrieved.Name);
            Assert.AreEqual(55, retrieved.Age);
        }
    }

    [TestMethod]
    public async Task GaldrDbAsync_DeleteAsync_DeletesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "DbAsyncDelete", Age = 60, Email = "dbasyncdelete@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            bool result = await db.DeleteAsync<Person>(id, PersonMeta.TypeInfo);

            Assert.IsTrue(result);

            Person retrieved = db.GetById<Person>(id, PersonMeta.TypeInfo);
            Assert.IsNull(retrieved);
        }
    }

    [TestMethod]
    public async Task GaldrDbAsync_GetDocumentAsync_RetrievesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "GetDocAsync", Age = 65, Email = "getdocasync@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            Person retrieved = await db.GetByIdAsync<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("GetDocAsync", retrieved.Name);
        }
    }

    #endregion
}
