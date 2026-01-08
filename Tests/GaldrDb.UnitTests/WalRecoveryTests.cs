using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
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
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            insertedId = db.InsertDocument("people", person);
        }

        // WAL file should exist after close
        Assert.IsTrue(File.Exists(walPath));

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetDocument<Person>("people", insertedId);

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
            db.CreateCollection("people");

            for (int i = 0; i < 10; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                ids[i] = db.InsertDocument("people", person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person retrieved = db.GetDocument<Person>("people", ids[i]);

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
            db.CreateCollection("people");
            Person person1 = new Person { Name = "First", Age = 25, Email = "first@example.com" };
            id1 = db.InsertDocument("people", person1);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person2 = new Person { Name = "Second", Age = 30, Email = "second@example.com" };
            id2 = db.InsertDocument("people", person2);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person person3 = new Person { Name = "Third", Age = 35, Email = "third@example.com" };
            id3 = db.InsertDocument("people", person3);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);
            Person retrieved3 = db.GetDocument<Person>("people", id3);

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
            db.CreateCollection("people");

            for (int i = 0; i < 50; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                db.InsertDocument("people", person);
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
            db.CreateCollection("people");
            Person person = new Person { Name = "Test Person", Age = 40, Email = "test@example.com" };
            insertedId = db.InsertDocument("people", person);

            db.Checkpoint();

            Person retrieved = db.GetDocument<Person>("people", insertedId);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Test Person", retrieved.Name);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetDocument<Person>("people", insertedId);
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
            db.CreateCollection("people");
            Person person = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            committedId = db.InsertDocument("people", person);
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
            Person retrieved = db.GetDocument<Person>("people", committedId);
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
            db.CreateCollection("people");
            Person person = new Person { Name = "NoWal Person", Age = 25, Email = "nowal@example.com" };
            id1 = db.InsertDocument("people", person);
        }

        // Open with WAL enabled
        GaldrDbOptions walOptions = new GaldrDbOptions { PageSize = 8192, UseWal = true };
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            // Original data should be accessible
            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Assert.IsNotNull(retrieved1);
            Assert.AreEqual("NoWal Person", retrieved1.Name);

            // Insert new data with WAL
            Person person2 = new Person { Name = "Wal Person", Age = 30, Email = "wal@example.com" };
            id2 = db.InsertDocument("people", person2);
        }

        // Reopen with WAL - both should be accessible
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, walOptions))
        {
            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);

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
            db.CreateCollection("people");

            Person person1 = new Person { Name = "Original Name", Age = 25, Email = "original@example.com" };
            id1 = db.InsertDocument("people", person1);

            Person person2 = new Person { Name = "To Be Deleted", Age = 30, Email = "delete@example.com" };
            id2 = db.InsertDocument("people", person2);

            // Update person1
            person1.Name = "Updated Name";
            person1.Age = 26;
            db.UpdateDocument("people", id1, person1);

            // Delete person2
            db.DeleteDocument("people", id2);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);

            Assert.IsNotNull(retrieved1);
            Assert.AreEqual("Updated Name", retrieved1.Name);
            Assert.AreEqual(26, retrieved1.Age);

            Assert.IsNull(retrieved2);
        }
    }
}
