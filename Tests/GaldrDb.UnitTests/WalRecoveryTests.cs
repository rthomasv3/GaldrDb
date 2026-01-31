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
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            insertedId = db.Insert(person);
        }

        // WAL file should exist after close
        Assert.IsTrue(File.Exists(walPath));

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);

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
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                ids[i] = db.Insert(person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person retrieved = db.GetById<Person>(ids[i]);

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
            Person person1 = new Person { Name = "First", Age = 25, Email = "first@example.com" };
            id1 = db.Insert(person1);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person2 = new Person { Name = "Second", Age = 30, Email = "second@example.com" };
            id2 = db.Insert(person2);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person3 = new Person { Name = "Third", Age = 35, Email = "third@example.com" };
            id3 = db.Insert(person3);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetById<Person>(id1);
            Person retrieved2 = db.GetById<Person>(id2);
            Person retrieved3 = db.GetById<Person>(id3);

            Assert.IsNotNull(retrieved1);
            Assert.IsNotNull(retrieved2);
            Assert.IsNotNull(retrieved3);
            Assert.AreEqual("First", retrieved1.Name);
            Assert.AreEqual("Second", retrieved2.Name);
            Assert.AreEqual("Third", retrieved3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_Checkpoint_WritesToBaseFile_KeepsWalIntact()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        string walPath = Path.Combine(_testDirectory, "test.wal");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int insertedId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Test", Age = 30, Email = "test@example.com" };
            insertedId = db.Insert(person);

            long walSizeBeforeCheckpoint = new FileInfo(walPath).Length;

            db.Checkpoint();

            long walSizeAfterCheckpoint = new FileInfo(walPath).Length;

            // WAL should not shrink after checkpoint (it's not truncated until reset)
            Assert.IsGreaterThanOrEqualTo(walSizeBeforeCheckpoint, walSizeAfterCheckpoint);

            // Data should still be accessible after checkpoint
            Person retrieved = db.GetById<Person>(insertedId);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }

        // After close and reopen with no WAL, data should still be in the base file
        // This verifies checkpoint actually wrote to the base file
        File.Delete(walPath);

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test", retrieved.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_DataAfterCheckpointAndNewTransaction_AllRecovered()
    {
        // Tests that data written before checkpoint, and data written after checkpoint
        // are both recovered correctly on reopen.
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;
        int id3;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert data before checkpoint
            id1 = db.Insert(new Person { Name = "First", Age = 25, Email = "first@example.com" });

            // Checkpoint writes to base file
            db.Checkpoint();

            // Insert more data after checkpoint (WAL may be reset on BeginTransaction)
            using (ITransaction tx = db.BeginTransaction())
            {
                id2 = tx.Insert(new Person { Name = "Second", Age = 30, Email = "second@example.com" });
                tx.Commit();
            }

            // Another insert
            id3 = db.Insert(new Person { Name = "Third", Age = 35, Email = "third@example.com" });
        }

        // Verify all documents accessible after reopen
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);
            Person r3 = db.GetById<Person>(id3);

            Assert.IsNotNull(r1);
            Assert.IsNotNull(r2);
            Assert.IsNotNull(r3);
            Assert.AreEqual("First", r1.Name);
            Assert.AreEqual("Second", r2.Name);
            Assert.AreEqual("Third", r3.Name);
        }
    }

    [TestMethod]
    public void WalRecovery_InterleavedAutoCommitAndTransaction_RecoveredInWalOrder()
    {
        // This test verifies that auto-commits (txId=0) interleaved with regular transactions
        // are recovered in WAL frame order, not txId order. This is critical because
        // txId=0 < txId=1, but the auto-commits may have been written AFTER txId=1's frames.
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true };

        int id1;
        int id2;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // First, create a transaction that writes some data
            using (ITransaction tx = db.BeginTransaction())
            {
                id1 = tx.Insert(new Person { Name = "TxPerson", Age = 25, Email = "tx@example.com" });
                tx.Commit();
            }

            // Now do an auto-commit update (simulating what happens in compaction or direct updates)
            // This creates frames with txId=0 that are written AFTER the transaction above
            Person person = db.GetById<Person>(id1);
            person.Name = "UpdatedByAutoCommit";
            person.Age = 30;
            db.Replace(person);

            // Insert another document via auto-commit
            id2 = db.Insert(new Person { Name = "AutoCommitPerson", Age = 35, Email = "auto@example.com" });
        }

        // Reopen and verify the auto-commit updates are preserved
        // If recovery applied frames in txId order instead of WAL order,
        // the earlier transaction's data would overwrite the auto-commit updates
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);

            Assert.IsNotNull(r1);
            Assert.AreEqual("UpdatedByAutoCommit", r1.Name);
            Assert.AreEqual(30, r1.Age);

            Assert.IsNotNull(r2);
            Assert.AreEqual("AutoCommitPerson", r2.Name);
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
            Person person = new Person { Name = "Test Person", Age = 40, Email = "test@example.com" };
            insertedId = db.Insert(person);

            db.Checkpoint();

            Person retrieved = db.GetById<Person>(insertedId);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test Person", retrieved.Name);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);
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
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person);
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
            Person retrieved = db.GetById<Person>(committedId);
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
            Person person = new Person { Name = "NoWal Person", Age = 25, Email = "nowal@example.com" };
            id1 = db.Insert(person);
        }

        // Open with WAL enabled
        GaldrDbOptions walOptions = new GaldrDbOptions { PageSize = 8192, UseWal = true };
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            // Original data should be accessible
            Person retrieved1 = db.GetById<Person>(id1);
            Assert.IsNotNull(retrieved1);
            Assert.AreEqual("NoWal Person", retrieved1.Name);

            // Insert new data with WAL
            Person person2 = new Person { Name = "Wal Person", Age = 30, Email = "wal@example.com" };
            id2 = db.Insert(person2);
        }

        // Reopen with WAL - both should be accessible
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            Person retrieved1 = db.GetById<Person>(id1);
            Person retrieved2 = db.GetById<Person>(id2);

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
            Person person1 = new Person { Name = "Original Name", Age = 25, Email = "original@example.com" };
            id1 = db.Insert(person1);

            Person person2 = new Person { Name = "To Be Deleted", Age = 30, Email = "delete@example.com" };
            id2 = db.Insert(person2);

            // Update person1
            person1.Id = id1;
            person1.Name = "Updated Name";
            person1.Age = 26;
            db.Replace<Person>(person1);

            // Delete person2
            db.DeleteById<Person>(id2);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetById<Person>(id1);
            Person retrieved2 = db.GetById<Person>(id2);

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
            using (ITransaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "TxPerson", Age = 35, Email = "txperson@example.com" };
                insertedId = tx.Insert(person);
                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);

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
            using (ITransaction tx = db.BeginTransaction())
            {
                Person person1 = new Person { Name = "Person1", Age = 25, Email = "p1@example.com" };
                Person person2 = new Person { Name = "Person2", Age = 30, Email = "p2@example.com" };
                Person person3 = new Person { Name = "Person3", Age = 35, Email = "p3@example.com" };

                id1 = tx.Insert(person1);
                id2 = tx.Insert(person2);
                id3 = tx.Insert(person3);

                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);
            Person r3 = db.GetById<Person>(id3);

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
            using (ITransaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
                insertedId = tx.Insert(person);
                tx.Commit();
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            // After reopen, VersionIndex should be rebuilt and transactions should work
            using (ITransaction tx = db.BeginTransaction())
            {
                Person retrieved = tx.GetById<Person>(insertedId);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Original", retrieved.Name);

                // Update within transaction
                retrieved.Name = "Updated";
                retrieved.Age = 30;
                bool updateResult = tx.Replace(retrieved);

                Assert.IsTrue(updateResult);
                tx.Commit();
            }

            // Verify update persisted
            Person finalResult = db.GetById<Person>(insertedId);
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
            id1 = db.Insert(new Person { Name = "First", Age = 25, Email = "first@example.com" });
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            id2 = db.Insert(new Person { Name = "Second", Age = 30, Email = "second@example.com" });
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            id3 = db.Insert(new Person { Name = "Third", Age = 35, Email = "third@example.com" });
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);
            Person r3 = db.GetById<Person>(id3);

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
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person);
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
            Person retrieved = db.GetById<Person>(committedId);
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
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.Insert(person);
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
            Person retrieved = db.GetById<Person>(committedId);
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
            id1 = db.Insert(new Person { Name = "First", Age = 25, Email = "first@example.com" });
            id2 = db.Insert(new Person { Name = "Second", Age = 30, Email = "second@example.com" });
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
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);

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
            
        }

        // Reopen and verify collection exists and is usable
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            int id = db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            Person retrieved = db.GetById<Person>(id);
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
            // Insert before checkpoint
            id1 = db.Insert(new Person { Name = "BeforeCheckpoint", Age = 25, Email = "before@example.com" });

            db.Checkpoint();

            // Insert after checkpoint
            id2 = db.Insert(new Person { Name = "AfterCheckpoint1", Age = 30, Email = "after1@example.com" });
            id3 = db.Insert(new Person { Name = "AfterCheckpoint2", Age = 35, Email = "after2@example.com" });
        }

        // Reopen - all data should be recovered
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person r1 = db.GetById<Person>(id1);
            Person r2 = db.GetById<Person>(id2);
            Person r3 = db.GetById<Person>(id3);

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
            insertedId = db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
            db.Checkpoint();
        }

        // Truncate WAL to empty (simulating checkpoint completed but WAL not deleted)
        File.WriteAllBytes(walPath, Array.Empty<byte>());

        // Reopen - should handle empty WAL gracefully
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(insertedId);
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
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 100; i++)
                {
                    ids[i] = tx.Insert(new Person
                    {
                        Name = $"Person{i}",
                        Age = 20 + i,
                        Email = $"person{i}@example.com"
                    });
                }
                tx.Commit();
            }
        }

        // Reopen - all 100 documents should be recovered
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 0; i < 100; i++)
            {
                Person retrieved = db.GetById<Person>(ids[i]);
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
            // Committed transaction
            committedId = db.Insert(new Person { Name = "Committed", Age = 25, Email = "committed@example.com" });

            // Rolled back transaction
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "RolledBack", Age = 30, Email = "rolledback@example.com" });
                tx.Rollback();
            }
        }

        // Reopen
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person committed = db.GetById<Person>(committedId);
            Assert.IsNotNull(committed);
            Assert.AreEqual("Committed", committed.Name);

            // Only one person should exist
            System.Collections.Generic.List<Person> all = db.Query<Person>().ToList();
            Assert.HasCount(1, all);
        }
    }
}
