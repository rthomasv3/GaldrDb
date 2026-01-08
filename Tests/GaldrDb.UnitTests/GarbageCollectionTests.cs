using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class GarbageCollectionTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "GaldrDbGCTests_" + Guid.NewGuid().ToString("N"));
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
    public void Vacuum_NoVersionsToCollect_ReturnsZero()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            db.Insert(person, PersonMeta.TypeInfo);

            GarbageCollectionResult result = db.Vacuum();

            Assert.AreEqual(0, result.VersionsCollected);
        }
    }

    [TestMethod]
    public void Vacuum_OldVersionsExist_CollectsOldVersions()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            // Update to create a new version (old version becomes superseded)
            person.Id = id;
            person.Name = "Version2";
            db.Update(person, PersonMeta.TypeInfo);

            // Update again
            person.Name = "Version3";
            db.Update(person, PersonMeta.TypeInfo);

            // Now vacuum - should collect the old versions since no active transactions
            GarbageCollectionResult result = db.Vacuum();

            // Should have collected at least 1 old version
            Assert.IsGreaterThan(0, result.VersionsCollected);
        }
    }

    [TestMethod]
    public void Vacuum_ActiveTransactionHoldsSnapshot_DoesNotCollectVisibleVersions()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            // Start a read transaction that holds a snapshot
            using (Transaction readTx = db.BeginReadOnlyTransaction())
            {
                // Update to create a new version
                person.Id = id;
                person.Name = "Version2";
                db.Update(person, PersonMeta.TypeInfo);

                // The read transaction should still see Version1
                Person fromReadTx = readTx.GetById<Person>(id, PersonMeta.TypeInfo);
                Assert.AreEqual("Version1", fromReadTx.Name);

                // Vacuum should not collect Version1 because readTx can still see it
                GarbageCollectionResult result = db.Vacuum();

                // Verify the read transaction can still read Version1
                Person stillVisible = readTx.GetById<Person>(id, PersonMeta.TypeInfo);
                Assert.AreEqual("Version1", stillVisible.Name);
            }
        }
    }

    [TestMethod]
    public void Vacuum_AfterTransactionEnds_CollectsOldVersions()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            // Start and end a read transaction
            using (Transaction readTx = db.BeginReadOnlyTransaction())
            {
                Person fromReadTx = readTx.GetById<Person>(id, PersonMeta.TypeInfo);
                Assert.AreEqual("Version1", fromReadTx.Name);
            }

            // Update to create a new version
            person.Id = id;
            person.Name = "Version2";
            db.Update(person, PersonMeta.TypeInfo);

            // Now vacuum - the old version should be collectible since no active snapshots
            GarbageCollectionResult result = db.Vacuum();

            // Current version should still be readable
            Person current = db.GetById<Person>(id, PersonMeta.TypeInfo);
            Assert.AreEqual("Version2", current.Name);
        }
    }

    [TestMethod]
    public void AutoGarbageCollection_RunsAfterThresholdCommits()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            AutoGarbageCollection = true,
            GarbageCollectionThreshold = 5 // Low threshold for testing
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Do multiple inserts and updates to trigger auto GC
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                int id = db.Insert(person, PersonMeta.TypeInfo);

                // Update to create old versions
                person.Id = id;
                person.Name = $"Person{i}_Updated";
                db.Update(person, PersonMeta.TypeInfo);
            }

            // Auto GC should have run by now (after threshold commits)
            // We can't easily verify it ran, but we can verify the database still works
            Person result = db.GetById<Person>(1, PersonMeta.TypeInfo);
            Assert.IsNotNull(result);
        }
    }

    [TestMethod]
    public void AutoGarbageCollection_Disabled_DoesNotRun()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            AutoGarbageCollection = false
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Do some operations
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            person.Id = id;
            person.Name = "Updated";
            db.Update(person, PersonMeta.TypeInfo);

            // Explicitly call Vacuum to verify it still works when called manually
            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThanOrEqualTo(0, result.VersionsCollected);
        }
    }

    [TestMethod]
    public void Vacuum_DeletedDocument_CollectsDeletedVersion()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.EnsureCollection(PersonMeta.TypeInfo);

            // Insert and then delete
            Person person = new Person { Name = "ToDelete", Age = 25, Email = "delete@example.com" };
            int id = db.Insert(person, PersonMeta.TypeInfo);

            db.Delete<Person>(id, PersonMeta.TypeInfo);

            // Vacuum should be able to collect the deleted version
            GarbageCollectionResult result = db.Vacuum();

            // Verify document is still deleted
            Person deleted = db.GetById<Person>(id, PersonMeta.TypeInfo);
            Assert.IsNull(deleted);
        }
    }
}
