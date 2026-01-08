using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.WAL;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class WalRecoveryTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbWalRecoveryTests_{Guid.NewGuid()}");
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
    public void WalRecovery_DataPersistsAcrossOpenClose_WithWal()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);
        }

        // WAL file should exist after close
        Assert.IsTrue(File.Exists(walPath));

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("John Doe", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);
        }
    }

    [TestMethod]
    public void WalRecovery_MultipleDocuments_AllRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int[] ids = new int[10];

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            for (int i = 0; i < 10; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                ids[i] = db.Insert(person, PersonMeta.TypeInfo);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person retrieved = db.GetById(ids[i], PersonMeta.TypeInfo);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual($"Person {i}", retrieved.Name);
                Assert.AreEqual(20 + i, retrieved.Age);
            }
        }
    }

    [TestMethod]
    public void WalRecovery_MultipleOpenCloseCycles_DataPersists()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person1 = new Person { Name = "First", Age = 25, Email = "first@example.com" };
            id1 = db.Insert(person1, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person2 = new Person { Name = "Second", Age = 30, Email = "second@example.com" };
            id2 = db.Insert(person2, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person3 = new Person { Name = "Third", Age = 35, Email = "third@example.com" };
            id3 = db.Insert(person3, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person retrieved2 = db.GetById(id2, PersonMeta.TypeInfo);
            Person retrieved3 = db.GetById(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved1);
            Assert.IsNotNull(retrieved2);
            Assert.IsNotNull(retrieved3);
            Assert.AreEqual("First", retrieved1.Name);
            Assert.AreEqual("Second", retrieved2.Name);
            Assert.AreEqual("Third", retrieved3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_Checkpoint_ReducesWalSize()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            for (int i = 0; i < 50; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                db.Insert(person, PersonMeta.TypeInfo);
            }

            long walSizeBeforeCheckpoint = new FileInfo(walPath).Length;

            db.Checkpoint();

            long walSizeAfterCheckpoint = new FileInfo(walPath).Length;

            Assert.IsLessThan(walSizeBeforeCheckpoint, walSizeAfterCheckpoint);
        }
    }

    [TestMethod]
    public void WalRecovery_AfterCheckpoint_DataStillAccessible()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "Test Person", Age = 40, Email = "test@example.com" };
            insertedId = db.Insert(person, PersonMeta.TypeInfo);

            db.Checkpoint();

            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test Person", retrieved.Name);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test Person", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_SimulateCrash_UncommittedDataLost()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int committedId;

        // Create database and insert a committed document
        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person, PersonMeta.TypeInfo);
        }

        // Now manually append an uncommitted frame to the WAL to simulate a crash
        // during a write operation (frame written but no commit flag)
        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            byte[] fakePageData = new byte[8192];
            fakePageData[0] = 0xFF; // Some marker data

            // Write frame WITHOUT commit flag - simulates crash mid-transaction
            wal.WriteFrame(999, 100, 0x01, fakePageData, WalFrameFlags.None);
            wal.Flush();
        }

        // Reopen - recovery should ignore the uncommitted frame
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            // Committed data should be recovered
            Person retrieved = db.GetById(committedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Committed", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_MixedWalAndNonWal_TransitionWorks()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        int id1;
        int id2;

        // Create without WAL
        GaldrDbOptions noWalOptions = new GaldrDbOptions { PageSize = 8192, UseWal = false };
        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, noWalOptions))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "NoWal Person", Age = 25, Email = "nowal@example.com" };
            id1 = db.Insert(person, PersonMeta.TypeInfo);
        }

        // Open with WAL enabled
        GaldrDbOptions walOptions = new GaldrDbOptions { PageSize = 8192, UseWal = true };
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            // Original data should be accessible
            Person retrieved1 = db.GetById(id1, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved1);
            Assert.AreEqual("NoWal Person", retrieved1.Name);

            // Insert new data with WAL
            Person person2 = new Person { Name = "Wal Person", Age = 30, Email = "wal@example.com" };
            id2 = db.Insert(person2, PersonMeta.TypeInfo);
        }

        // Reopen with WAL - both should be accessible
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            Person retrieved1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person retrieved2 = db.GetById(id2, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved1);
            Assert.IsNotNull(retrieved2);
            Assert.AreEqual("NoWal Person", retrieved1.Name);
            Assert.AreEqual("Wal Person", retrieved2.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_UpdateAndDelete_RecoveredCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person1 = new Person { Name = "Original Name", Age = 25, Email = "original@example.com" };
            id1 = db.Insert(person1, PersonMeta.TypeInfo);

            Person person2 = new Person { Name = "To Be Deleted", Age = 30, Email = "delete@example.com" };
            id2 = db.Insert(person2, PersonMeta.TypeInfo);

            // Update person1
            person1.Id = id1;
            person1.Name = "Updated Name";
            person1.Age = 26;
            db.Update(person1, PersonMeta.TypeInfo);

            // Delete person2
            db.Delete<Person>(id2, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person retrieved2 = db.GetById(id2, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved1);
            Assert.AreEqual("Updated Name", retrieved1.Name);
            Assert.AreEqual(26, retrieved1.Age);

            Assert.IsNull(retrieved2);
        }
    }

    [TestMethod]
    public void WalRecovery_TransactionBasedInsert_RecoveredCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "TxPerson", Age = 35, Email = "txperson@example.com" };
                insertedId = tx.Insert(person, PersonMeta.TypeInfo);
                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("TxPerson", retrieved.Name);
            Assert.AreEqual(35, retrieved.Age);
        }
    }

    [TestMethod]
    public void WalRecovery_TransactionBasedMultipleInserts_AllRecovered()
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
                Person person2 = new Person { Name = "Person2", Age = 30, Email = "p2@example.com" };
                Person person3 = new Person { Name = "Person3", Age = 35, Email = "p3@example.com" };

                id1 = tx.Insert(person1, PersonMeta.TypeInfo);
                id2 = tx.Insert(person2, PersonMeta.TypeInfo);
                id3 = tx.Insert(person3, PersonMeta.TypeInfo);

                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById(id2, PersonMeta.TypeInfo);
            Person r3 = db.GetById(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
            Assert.AreEqual("Person1", r1.Name);
            Assert.AreEqual("Person2", r2.Name);
            Assert.AreEqual("Person3", r3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_VersionIndexRebuilt_TransactionsWorkAfterReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
                insertedId = tx.Insert(person, PersonMeta.TypeInfo);
                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            // After reopen, VersionIndex should be rebuilt and transactions should work
            using (Transaction tx = db.BeginTransaction())
            {
                Person retrieved = tx.GetById(insertedId, PersonMeta.TypeInfo);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Original", retrieved.Name);

                // Update within transaction
                retrieved.Name = "Updated";
                retrieved.Age = 30;
                bool updateResult = tx.Update(retrieved, PersonMeta.TypeInfo);

                Assert.IsTrue(updateResult);
                tx.Commit();
            }

            // Verify update persisted
            Person finalResult = db.GetById(insertedId, PersonMeta.TypeInfo);
            Assert.AreEqual("Updated", finalResult.Name);
            Assert.AreEqual(30, finalResult.Age);
        }
    }

    [TestMethod]
    public void WalRecovery_MultipleTransactionsAcrossReopens_AllRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            id1 = db.Insert(new Person { Name = "First", Age = 25, Email = "first@example.com" }, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            id2 = db.Insert(new Person { Name = "Second", Age = 30, Email = "second@example.com" }, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            id3 = db.Insert(new Person { Name = "Third", Age = 35, Email = "third@example.com" }, PersonMeta.TypeInfo);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById(id2, PersonMeta.TypeInfo);
            Person r3 = db.GetById(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
            Assert.AreEqual("First", r1.Name);
            Assert.AreEqual("Second", r2.Name);
            Assert.AreEqual("Third", r3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_PartialWalFrame_IgnoredOnRecovery()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int committedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person, PersonMeta.TypeInfo);
        }

        // Append partial/truncated data to WAL to simulate crash mid-write
        using (FileStream fs = new FileStream(walPath, FileMode.Append, FileAccess.Write))
        {
            // Write partial frame header (less than a full frame)
            byte[] partialData = new byte[50];
            for (int i = 0; i < partialData.Length; i++)
            {
                partialData[i] = 0xAB;
            }
            fs.Write(partialData, 0, partialData.Length);
        }

        // Reopen - recovery should handle partial frame gracefully
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(committedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Committed", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_MultipleUncommittedTransactions_AllIgnored()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int committedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person, PersonMeta.TypeInfo);
        }

        // Append multiple uncommitted transactions
        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            byte[] fakePageData = new byte[8192];

            // Transaction 100 - uncommitted
            fakePageData[0] = 0x01;
            wal.WriteFrame(100, 50, 0x01, fakePageData, WalFrameFlags.None);
            wal.WriteFrame(100, 51, 0x01, fakePageData, WalFrameFlags.None);

            // Transaction 101 - uncommitted
            fakePageData[0] = 0x02;
            wal.WriteFrame(101, 52, 0x01, fakePageData, WalFrameFlags.None);

            // Transaction 102 - uncommitted
            fakePageData[0] = 0x03;
            wal.WriteFrame(102, 53, 0x01, fakePageData, WalFrameFlags.None);
            wal.WriteFrame(102, 54, 0x01, fakePageData, WalFrameFlags.None);
            wal.WriteFrame(102, 55, 0x01, fakePageData, WalFrameFlags.None);

            wal.Flush();
        }

        // Reopen - all uncommitted transactions should be ignored
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(committedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Committed", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_InterleavedCommittedAndUncommitted_OnlyCommittedRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            id1 = db.Insert(new Person { Name = "First", Age = 25, Email = "first@example.com" }, PersonMeta.TypeInfo);
            id2 = db.Insert(new Person { Name = "Second", Age = 30, Email = "second@example.com" }, PersonMeta.TypeInfo);
        }

        // Append interleaved committed and uncommitted transactions
        using (WriteAheadLog wal = new WriteAheadLog(walPath, 8192))
        {
            wal.Open();

            byte[] fakePageData = new byte[8192];

            // Uncommitted transaction 100
            fakePageData[0] = 0x01;
            wal.WriteFrame(100, 60, 0x01, fakePageData, WalFrameFlags.None);

            // Committed transaction 101
            fakePageData[0] = 0x02;
            wal.WriteFrame(101, 61, 0x01, fakePageData, WalFrameFlags.None);
            wal.WriteFrame(101, 62, 0x01, fakePageData, WalFrameFlags.Commit);

            // More uncommitted from transaction 100
            fakePageData[0] = 0x03;
            wal.WriteFrame(100, 63, 0x01, fakePageData, WalFrameFlags.None);

            // Another uncommitted transaction 102
            fakePageData[0] = 0x04;
            wal.WriteFrame(102, 64, 0x01, fakePageData, WalFrameFlags.None);

            wal.Flush();
        }

        // Reopen - original data should still be accessible
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById(id2, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.AreEqual("First", r1.Name);
            Assert.AreEqual("Second", r2.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_DDLRecovery_CollectionSurvivesCrash()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
        }

        // Reopen and verify collection exists and is usable
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            int id = db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" }, PersonMeta.TypeInfo);

            Person retrieved = db.GetById(id, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_AfterCheckpointThenMoreWrites_AllDataRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert before checkpoint
            id1 = db.Insert(new Person { Name = "BeforeCheckpoint", Age = 25, Email = "before@example.com" }, PersonMeta.TypeInfo);

            db.Checkpoint();

            // Insert after checkpoint
            id2 = db.Insert(new Person { Name = "AfterCheckpoint1", Age = 30, Email = "after1@example.com" }, PersonMeta.TypeInfo);
            id3 = db.Insert(new Person { Name = "AfterCheckpoint2", Age = 35, Email = "after2@example.com" }, PersonMeta.TypeInfo);
        }

        // Reopen - all data should be recovered
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById(id1, PersonMeta.TypeInfo);
            Person r2 = db.GetById(id2, PersonMeta.TypeInfo);
            Person r3 = db.GetById(id3, PersonMeta.TypeInfo);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
            Assert.AreEqual("BeforeCheckpoint", r1.Name);
            Assert.AreEqual("AfterCheckpoint1", r2.Name);
            Assert.AreEqual("AfterCheckpoint2", r3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_EmptyWalFile_HandledGracefully()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);
            insertedId = db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" }, PersonMeta.TypeInfo);
            db.Checkpoint();
        }

        // Truncate WAL to empty (simulating checkpoint completed but WAL not deleted)
        File.WriteAllBytes(walPath, Array.Empty<byte>());

        // Reopen - should handle empty WAL gracefully
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById(insertedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_LargeTransaction_ManyDocumentsRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int[] ids = new int[100];

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 100; i++)
                {
                    ids[i] = tx.Insert(new Person
                    {
                        Name = $"Person{i}",
                        Age = 20 + i,
                        Email = $"person{i}@example.com"
                    }, PersonMeta.TypeInfo);
                }
                tx.Commit();
            }
        }

        // Reopen - all 100 documents should be recovered
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 0; i < 100; i++)
            {
                Person retrieved = db.GetById(ids[i], PersonMeta.TypeInfo);
                Assert.IsNotNull(retrieved, $"Person {i} should exist");
                Assert.AreEqual($"Person{i}", retrieved.Name);
            }
        }
    }

    [TestMethod]
    public void WalRecovery_RolledBackTransaction_NotRecovered()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int committedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Committed transaction
            committedId = db.Insert(new Person { Name = "Committed", Age = 25, Email = "committed@example.com" }, PersonMeta.TypeInfo);

            // Rolled back transaction
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "RolledBack", Age = 30, Email = "rolledback@example.com" }, PersonMeta.TypeInfo);
                tx.Rollback();
            }
        }

        // Reopen
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person committed = db.GetById(committedId, PersonMeta.TypeInfo);
            Assert.IsNotNull(committed);
            Assert.AreEqual("Committed", committed.Name);

            // Only one person should exist
            System.Collections.Generic.List<Person> all = db.Query(PersonMeta.TypeInfo).ToList();
            Assert.HasCount(1, all);
        }
    }
}
