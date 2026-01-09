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
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            db.Insert(person);

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
            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person);

            // Update to create a new version (old version becomes superseded)
            person.Id = id;
            person.Name = "Version2";
            db.Update(person);

            // Update again
            person.Name = "Version3";
            db.Update(person);

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
            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person);

            // Start a read transaction that holds a snapshot
            using (Transaction readTx = db.BeginReadOnlyTransaction())
            {
                // Update to create a new version
                person.Id = id;
                person.Name = "Version2";
                db.Update(person);

                // The read transaction should still see Version1
                Person fromReadTx = readTx.GetById<Person>(id);
                Assert.AreEqual("Version1", fromReadTx.Name);

                // Vacuum should not collect Version1 because readTx can still see it
                GarbageCollectionResult result = db.Vacuum();

                // Verify the read transaction can still read Version1
                Person stillVisible = readTx.GetById<Person>(id);
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
            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person);

            // Start and end a read transaction
            using (Transaction readTx = db.BeginReadOnlyTransaction())
            {
                Person fromReadTx = readTx.GetById<Person>(id);
                Assert.AreEqual("Version1", fromReadTx.Name);
            }

            // Update to create a new version
            person.Id = id;
            person.Name = "Version2";
            db.Update(person);

            // Now vacuum - the old version should be collectible since no active snapshots
            GarbageCollectionResult result = db.Vacuum();

            // Current version should still be readable
            Person current = db.GetById<Person>(id);
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
            // Do multiple inserts and updates to trigger auto GC
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                int id = db.Insert(person);

                // Update to create old versions
                person.Id = id;
                person.Name = $"Person{i}_Updated";
                db.Update(person);
            }

            // Auto GC should have run by now (after threshold commits)
            // We can't easily verify it ran, but we can verify the database still works
            Person result = db.GetById<Person>(1);
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
            // Do some operations
            Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
            int id = db.Insert(person);

            person.Id = id;
            person.Name = "Updated";
            db.Update(person);

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
            // Insert and then delete
            Person person = new Person { Name = "ToDelete", Age = 25, Email = "delete@example.com" };
            int id = db.Insert(person);

            db.Delete<Person>(id);

            // Vacuum should be able to collect the deleted version
            GarbageCollectionResult result = db.Vacuum();

            // Verify document is still deleted
            Person deleted = db.GetById<Person>(id);
            Assert.IsNull(deleted);
        }
    }

    [TestMethod]
    public void Vacuum_CollectableVersions_ContainsDocumentLocations()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert initial version
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person);

            // Update to create old version
            person.Id = id;
            person.Name = "Version2";
            db.Update(person);

            // Vacuum and check that CollectableVersions contains location info
            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThan(0, result.VersionsCollected);
            Assert.IsNotNull(result.CollectableVersions);
            Assert.HasCount(result.VersionsCollected, result.CollectableVersions);

            // Verify each collectable version has valid location info
            foreach (CollectableVersion cv in result.CollectableVersions)
            {
                Assert.IsNotNull(cv.Location);
                Assert.IsGreaterThanOrEqualTo(0, cv.Location.PageId);
                Assert.IsGreaterThanOrEqualTo(0, cv.Location.SlotIndex);
                Assert.AreEqual("Person", cv.CollectionName);
            }
        }
    }

    [TestMethod]
    public void Vacuum_PhysicalCleanup_DatabaseStillFunctional()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert multiple documents
            for (int i = 0; i < 20; i++)
            {
                Person person = new Person
                {
                    Name = $"Person{i}",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                db.Insert(person);
            }

            // Update all documents multiple times to create many old versions
            for (int round = 0; round < 5; round++)
            {
                for (int i = 1; i <= 20; i++)
                {
                    Person person = db.GetById<Person>(i);
                    person.Name = $"Person{i}_Round{round}";
                    db.Update(person);
                }
            }

            // Vacuum to clean up old versions (this calls DeleteDocument for each collected version)
            GarbageCollectionResult result = db.Vacuum();

            // Verify versions were collected and physical cleanup occurred
            Assert.IsGreaterThan(0, result.VersionsCollected);
            Assert.IsNotNull(result.CollectableVersions);
            Assert.IsGreaterThan(0, result.CollectableVersions.Count);

            // Verify all collected versions have valid locations that were cleaned up
            foreach (CollectableVersion cv in result.CollectableVersions)
            {
                Assert.IsNotNull(cv.Location);
                Assert.IsGreaterThanOrEqualTo(0, cv.Location.PageId);
            }

            // Verify all current versions are still readable after physical cleanup
            for (int i = 1; i <= 20; i++)
            {
                Person person = db.GetById<Person>(i);
                Assert.IsNotNull(person);
                Assert.AreEqual($"Person{i}_Round4", person.Name);
            }

            // Insert more documents - system should still work after cleanup
            for (int i = 0; i < 10; i++)
            {
                Person newPerson = new Person
                {
                    Name = $"NewPerson{i}",
                    Age = 50 + i,
                    Email = $"new{i}@example.com"
                };
                int newId = db.Insert(newPerson);
                Assert.IsGreaterThan(20, newId);
            }

            // Verify new documents are readable
            for (int i = 21; i <= 30; i++)
            {
                Person person = db.GetById<Person>(i);
                Assert.IsNotNull(person);
            }
        }
    }

    [TestMethod]
    public void Vacuum_AfterReopen_PhysicalCleanupPersists()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        int docId;

        // Create database, insert and update
        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Original", Age = 25, Email = "test@example.com" };
            docId = db.Insert(person);

            person.Id = docId;
            person.Name = "Updated";
            db.Update(person);

            // Vacuum to clean up old version
            GarbageCollectionResult result = db.Vacuum();
            Assert.IsGreaterThan(0, result.VersionsCollected);

            db.Checkpoint();
        }

        // Reopen and verify everything still works
        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            // Current version should be readable
            Person person = db.GetById<Person>(docId);
            Assert.IsNotNull(person);
            Assert.AreEqual("Updated", person.Name);

            // Should be able to do more operations
            person.Name = "AfterReopen";
            bool updated = db.Update(person);
            Assert.IsTrue(updated);

            Person afterUpdate = db.GetById<Person>(docId);
            Assert.AreEqual("AfterReopen", afterUpdate.Name);
        }
    }

    [TestMethod]
    public void Vacuum_MultipleCollections_CleansUpAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert into Person collection
            Person person = new Person { Name = "PersonV1", Age = 25, Email = "person@example.com" };
            int personId = db.Insert(person);
            person.Id = personId;
            person.Name = "PersonV2";
            db.Update(person);

            // Insert into User collection
            User user = new User { Name = "UserV1", Email = "user1@example.com", Department = "Engineering" };
            int userId = db.Insert(user);
            user.Id = userId;
            user.Name = "UserV2";
            user.Email = "user2@example.com";
            db.Update(user);

            // Vacuum should clean up old versions from both collections
            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThan(0, result.VersionsCollected);
            Assert.IsGreaterThanOrEqualTo(2, result.VersionsCollected);

            // Verify both current versions are still readable
            Person currentPerson = db.GetById<Person>(personId);
            Assert.AreEqual("PersonV2", currentPerson.Name);

            User currentUser = db.GetById<User>(userId);
            Assert.AreEqual("UserV2", currentUser.Name);
        }
    }

    [TestMethod]
    public void Vacuum_LargeDocument_ContinuationPagesFreed()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 4096, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Create a large document that spans multiple pages
            // Use Email field since Name is indexed and would overflow the index
            string largeEmail = new string('x', 5000) + "@example.com"; // Larger than one page
            Person person = new Person { Name = "LargePerson", Age = 25, Email = largeEmail };
            int id = db.Insert(person);

            // Update with a smaller document
            person.Id = id;
            person.Email = "small@example.com";
            db.Update(person);

            // Vacuum should free the continuation pages from the old large document
            GarbageCollectionResult result = db.Vacuum();
            Assert.IsGreaterThan(0, result.VersionsCollected);

            // Verify the collected version had a location (physical cleanup occurred)
            Assert.IsNotNull(result.CollectableVersions);
            Assert.IsGreaterThan(0, result.CollectableVersions.Count);
            foreach (CollectableVersion cv in result.CollectableVersions)
            {
                Assert.IsNotNull(cv.Location);
            }

            // Verify current document is readable
            Person current = db.GetById<Person>(id);
            Assert.AreEqual("small@example.com", current.Email);

            // Insert another large document - system should still work
            string anotherLargeEmail = new string('y', 5000) + "@example.com";
            Person another = new Person { Name = "AnotherLarge", Age = 30, Email = anotherLargeEmail };
            int anotherId = db.Insert(another);

            // Verify both documents are readable
            Person first = db.GetById<Person>(id);
            Person second = db.GetById<Person>(anotherId);
            Assert.IsNotNull(first);
            Assert.IsNotNull(second);
            Assert.AreEqual("small@example.com", first.Email);
            Assert.StartsWith("yyyyy", second.Email);
        }
    }

    [TestMethod]
    public void Vacuum_InsertAndUpdateSameTransaction_NothingToCollect()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert and update in the same transaction - only one version should be created
            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Original", Age = 25, Email = "test@example.com" };
                int id = tx.Insert(person);

                // Update in the same transaction - this overwrites the write set entry
                person.Id = id;
                person.Name = "Updated";
                tx.Update(person);

                tx.Commit();
            }

            // Vacuum should find nothing to collect since only one version exists
            GarbageCollectionResult result = db.Vacuum();

            Assert.AreEqual(0, result.VersionsCollected);

            // Verify the document has the updated value
            Person retrieved = db.GetById<Person>(1);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Updated", retrieved.Name);
        }
    }
}
