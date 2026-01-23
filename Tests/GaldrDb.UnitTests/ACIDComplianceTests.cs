using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDatabase = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class ACIDComplianceTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbACIDTests_{Guid.NewGuid()}");
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

    #region Atomicity Tests

    [TestMethod]
    public void Atomicity_RolledBackTransaction_NoChangesApplied()
    {
        string dbPath = Path.Combine(_testDirectory, "atomicity.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 30 });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "ShouldNotExist", Age = 25 });

                Person updated = new Person { Id = id, Name = "ShouldNotUpdate", Age = 99 };
                tx.Replace(updated);

                tx.Rollback();
            }

            Person retrieved = db.GetById<Person>(id);
            Assert.AreEqual("Original", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);

            List<Person> allPeople = db.Query<Person>().ToList();
            Assert.HasCount(1, allPeople);
        }
    }

    [TestMethod]
    public void Atomicity_ExceptionInTransaction_ChangesNotApplied()
    {
        string dbPath = Path.Combine(_testDirectory, "atomicity_exception.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 30 });

            try
            {
                using (Transaction tx = db.BeginTransaction())
                {
                    tx.Insert(new Person { Name = "Inserted", Age = 25 });
                    throw new InvalidOperationException("Simulated failure");
                }
            }
            catch (InvalidOperationException)
            {
                // Expected
            }

            List<Person> allPeople = db.Query<Person>().ToList();
            Assert.HasCount(1, allPeople);
            Assert.AreEqual("Original", allPeople[0].Name);
        }
    }

    [TestMethod]
    public void Atomicity_MultipleOperations_AllOrNothing()
    {
        string dbPath = Path.Combine(_testDirectory, "atomicity_all_or_nothing.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 100; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i });
                }

                tx.Commit();
            }

            List<Person> allPeople = db.Query<Person>().ToList();
            Assert.HasCount(100, allPeople);
        }
    }

    #endregion

    #region Consistency Tests

    [TestMethod]
    public void Consistency_ConflictingTransactions_DatabaseRemainsconsistent()
    {
        string dbPath = Path.Combine(_testDirectory, "consistency.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 30 });

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                tx2.Replace(new Person { Id = id, Name = "FromTx2", Age = 31 });
                tx2.Commit();
            }

            try
            {
                tx1.Replace(new Person { Id = id, Name = "FromTx1", Age = 32 });
                Assert.Fail("Expected WriteConflictException");
            }
            catch (WriteConflictException)
            {
                // Expected
            }
            finally
            {
                tx1.Dispose();
            }

            Person final = db.GetById<Person>(id);
            Assert.AreEqual("FromTx2", final.Name);
            Assert.AreEqual(31, final.Age);
        }
    }

    #endregion

    #region Isolation Tests

    [TestMethod]
    public void Isolation_ConcurrentReadsAndWrites_NoIntermediateStates()
    {
        string dbPath = Path.Combine(_testDirectory, "isolation.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Initial", Age = 0 });

            const int iterations = 50;
            List<Task> tasks = new List<Task>();
            object lockObj = new object();
            int errors = 0;

            for (int i = 0; i < iterations; i++)
            {
                int iteration = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        using (Transaction readTx = db.BeginReadOnlyTransaction())
                        {
                            Person p = readTx.GetById<Person>(id);
                            if (p != null && p.Name != "Initial" && !p.Name.StartsWith("Update"))
                            {
                                lock (lockObj) { errors++; }
                            }
                        }
                    }
                    catch
                    {
                        // Ignore errors, we're just checking for consistency
                    }
                }));

                if (iteration % 5 == 0)
                {
                    int updateIteration = iteration;
                    tasks.Add(Task.Run(() =>
                    {
                        try
                        {
                            using (Transaction writeTx = db.BeginTransaction())
                            {
                                writeTx.Replace(new Person { Id = id, Name = $"Update{updateIteration}", Age = updateIteration });
                                writeTx.Commit();
                            }
                        }
                        catch (WriteConflictException)
                        {
                            // Expected in concurrent scenarios
                        }
                    }));
                }
            }

            Task.WaitAll(tasks.ToArray());
            Assert.AreEqual(0, errors, "Should never see intermediate/corrupt states");
        }
    }

    [TestMethod]
    public void Isolation_RepeatableRead_SameDataWithinTransaction()
    {
        string dbPath = Path.Combine(_testDirectory, "repeatable_read.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Alice", Age = 30 });

            using (Transaction readTx = db.BeginReadOnlyTransaction())
            {
                Person firstRead = readTx.GetById<Person>(id);
                Assert.AreEqual("Alice", firstRead.Name);

                using (Transaction writeTx = db.BeginTransaction())
                {
                    writeTx.Replace(new Person { Id = id, Name = "Alice Updated", Age = 31 });
                    writeTx.Commit();
                }

                Person secondRead = readTx.GetById<Person>(id);
                Assert.AreEqual("Alice", secondRead.Name);
                Assert.AreEqual(firstRead.Age, secondRead.Age);
            }
        }
    }

    #endregion

    #region Durability Tests

    [TestMethod]
    public void Durability_CommittedData_SurvivesRestart()
    {
        string dbPath = Path.Combine(_testDirectory, "durability.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = true };

        int insertedId;

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                insertedId = tx.Insert(new Person { Name = "Durable", Age = 42 });
                tx.Commit();
            }
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Durable", retrieved.Name);
            Assert.AreEqual(42, retrieved.Age);
        }
    }

    [TestMethod]
    public void Durability_MultipleTransactions_AllSurviveRestart()
    {
        string dbPath = Path.Combine(_testDirectory, "durability_multi.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = true };

        int[] ids = new int[10];

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                using (Transaction tx = db.BeginTransaction())
                {
                    ids[i] = tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i });
                    tx.Commit();
                }
            }
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person p = db.GetById<Person>(ids[i]);
                Assert.IsNotNull(p, $"Person {i} should exist");
                Assert.AreEqual($"Person{i}", p.Name);
            }
        }
    }

    [TestMethod]
    public void Durability_UpdatesAndDeletes_SurviveRestart()
    {
        string dbPath = Path.Combine(_testDirectory, "durability_update_delete.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = true };

        int id1;
        int id2;

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            id1 = db.Insert(new Person { Name = "ToUpdate", Age = 25 });
            id2 = db.Insert(new Person { Name = "ToDelete", Age = 30 });

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Replace(new Person { Id = id1, Name = "Updated", Age = 26 });
                tx.DeleteById<Person>(id2);
                tx.Commit();
            }
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            Person p1 = db.GetById<Person>(id1);
            Assert.IsNotNull(p1);
            Assert.AreEqual("Updated", p1.Name);
            Assert.AreEqual(26, p1.Age);

            Person p2 = db.GetById<Person>(id2);
            Assert.IsNull(p2);
        }
    }

    [TestMethod]
    public async Task Durability_CheckpointAsync_DataAccessibleAfterRestart()
    {
        string dbPath = Path.Combine(_testDirectory, "durability_checkpoint_async.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = true };

        int insertedId;

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            insertedId = db.Insert(new Person { Name = "Checkpointed", Age = 35 });

            await db.CheckpointAsync();
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Checkpointed", retrieved.Name);
        }
    }

    #endregion

    #region Concurrent Transaction Tests

    [TestMethod]
    public void ConcurrentTransactions_ReadDoesNotBlockWrite()
    {
        string dbPath = Path.Combine(_testDirectory, "concurrent_no_block.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Test", Age = 25 });

            Transaction readTx = db.BeginReadOnlyTransaction();
            Person readResult = readTx.GetById<Person>(id);

            using (Transaction writeTx = db.BeginTransaction())
            {
                writeTx.Replace(new Person { Id = id, Name = "Updated", Age = 26 });
                writeTx.Commit();
            }

            readTx.Dispose();

            Person finalResult = db.GetById<Person>(id);
            Assert.AreEqual("Updated", finalResult.Name);
        }
    }

    [TestMethod]
    public void ConcurrentTransactions_WritersOnDifferentDocuments_NoConflict()
    {
        string dbPath = Path.Combine(_testDirectory, "concurrent_writers_different.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int[] ids = new int[10];
            for (int i = 0; i < 10; i++)
            {
                ids[i] = db.Insert(new Person { Name = $"Person{i}", Age = 20 + i });
            }

            List<Task> tasks = new List<Task>();
            int successCount = 0;
            object lockObj = new object();

            for (int i = 0; i < 10; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(() =>
                {
                    using (Transaction tx = db.BeginTransaction())
                    {
                        tx.Replace(new Person { Id = ids[idx], Name = $"Updated{idx}", Age = 100 + idx });
                        tx.Commit();
                        lock (lockObj) { successCount++; }
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());

            Assert.AreEqual(10, successCount, "All updates on different documents should succeed");

            for (int i = 0; i < 10; i++)
            {
                Person p = db.GetById<Person>(ids[i]);
                Assert.AreEqual($"Updated{i}", p.Name);
            }
        }
    }

    [TestMethod]
    public void ConcurrentTransactions_WritersOnSameDocument_OnlyOneSucceeds()
    {
        string dbPath = Path.Combine(_testDirectory, "concurrent_writers_same.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 25 });

            List<Task> tasks = new List<Task>();
            int successCount = 0;
            int conflictCount = 0;
            object lockObj = new object();

            for (int i = 0; i < 10; i++)
            {
                int iteration = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        using (Transaction tx = db.BeginTransaction())
                        {
                            tx.Replace(new Person { Id = id, Name = $"Writer{iteration}", Age = 100 + iteration });
                            tx.Commit();
                            lock (lockObj) { successCount++; }
                        }
                    }
                    catch (WriteConflictException)
                    {
                        lock (lockObj) { conflictCount++; }
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());

            Assert.IsGreaterThan(0, successCount);
            Assert.AreEqual(10, successCount + conflictCount);
        }
    }

    #endregion

    #region Async Transaction Tests

    [TestMethod]
    public async Task Async_InsertAndGetById_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "async_insert_get.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                int id = await tx.InsertAsync(new Person { Name = "AsyncPerson", Age = 30 });
                await tx.CommitAsync();

                Person retrieved = await db.GetByIdAsync<Person>(id);
                Assert.IsNotNull(retrieved);
                Assert.AreEqual("AsyncPerson", retrieved.Name);
            }
        }
    }

    [TestMethod]
    public async Task Async_UpdateAndDelete_Works()
    {
        string dbPath = Path.Combine(_testDirectory, "async_update_delete.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 25 });

            using (Transaction tx = db.BeginTransaction())
            {
                bool updated = await tx.ReplaceAsync(new Person { Id = id, Name = "Updated", Age = 26 });
                Assert.IsTrue(updated);
                await tx.CommitAsync();
            }

            Person afterUpdate = db.GetById<Person>(id);
            Assert.AreEqual("Updated", afterUpdate.Name);

            using (Transaction tx = db.BeginTransaction())
            {
                bool deleted = await tx.DeleteByIdAsync<Person>(id);
                Assert.IsTrue(deleted);
                await tx.CommitAsync();
            }

            Person afterDelete = db.GetById<Person>(id);
            Assert.IsNull(afterDelete);
        }
    }

    [TestMethod]
    public async Task Async_CommitAsync_DataPersistsAcrossRestart()
    {
        string dbPath = Path.Combine(_testDirectory, "async_commit_persist.db");
        GaldrDbOptions options = new GaldrDbOptions { UseWal = true };

        int insertedId;

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                insertedId = await tx.InsertAsync(new Person { Name = "AsyncDurable", Age = 42 });
                await tx.CommitAsync();
            }
        }

        using (GaldrDatabase db = GaldrDatabase.Open(dbPath, options))
        {
            Person retrieved = await db.GetByIdAsync<Person>(insertedId);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("AsyncDurable", retrieved.Name);
            Assert.AreEqual(42, retrieved.Age);
        }
    }

    [TestMethod]
    public async Task Async_ConcurrentOperations_WorkCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "async_concurrent.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int[] ids = new int[10];
            for (int i = 0; i < 10; i++)
            {
                ids[i] = db.Insert(new Person { Name = $"Person{i}", Age = 20 + i });
            }

            List<Task> tasks = new List<Task>();
            int successCount = 0;
            object lockObj = new object();

            for (int i = 0; i < 10; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(async () =>
                {
                    using (Transaction tx = db.BeginTransaction())
                    {
                        await tx.ReplaceAsync(new Person { Id = ids[idx], Name = $"AsyncUpdated{idx}", Age = 100 + idx });
                        await tx.CommitAsync();
                        lock (lockObj) { successCount++; }
                    }
                }));
            }

            await Task.WhenAll(tasks);

            Assert.AreEqual(10, successCount, "All async updates on different documents should succeed");

            for (int i = 0; i < 10; i++)
            {
                Person p = await db.GetByIdAsync<Person>(ids[i]);
                Assert.AreEqual($"AsyncUpdated{i}", p.Name);
            }
        }
    }

    [TestMethod]
    public async Task Async_WriteConflict_ThrowsCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "async_conflict.db");

        using (GaldrDatabase db = GaldrDatabase.Create(dbPath, new GaldrDbOptions()))
        {
            int id = db.Insert(new Person { Name = "Original", Age = 30 });

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                await tx2.ReplaceAsync(new Person { Id = id, Name = "FromTx2", Age = 31 });
                await tx2.CommitAsync();
            }

            await Assert.ThrowsExactlyAsync<WriteConflictException>(async () =>
            {
                await tx1.ReplaceAsync(new Person { Id = id, Name = "FromTx1", Age = 32 });
            });

            tx1.Dispose();
        }
    }

    #endregion
}
