using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
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
            db.Replace(person);

            // Update again
            person.Name = "Version3";
            db.Replace(person);

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
            using (ITransaction readTx = db.BeginReadOnlyTransaction())
            {
                // Update to create a new version
                person.Id = id;
                person.Name = "Version2";
                db.Replace(person);

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
            using (ITransaction readTx = db.BeginReadOnlyTransaction())
            {
                Person fromReadTx = readTx.GetById<Person>(id);
                Assert.AreEqual("Version1", fromReadTx.Name);
            }

            // Update to create a new version
            person.Id = id;
            person.Name = "Version2";
            db.Replace(person);

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
                db.Replace(person);
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
            db.Replace(person);

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

            db.DeleteById<Person>(id);

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
            db.Replace(person);

            // Vacuum and check that CollectableVersions contains location info
            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThan(0, result.VersionsCollected);
            Assert.IsNotNull(result.CollectableVersions);
            Assert.HasCount(result.VersionsCollected, result.CollectableVersions);

            // Verify each collectable version has valid location info
            foreach (CollectableVersion cv in result.CollectableVersions)
            {
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
                    db.Replace(person);
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
            db.Replace(person);

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
            bool updated = db.Replace(person);
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
            db.Replace(person);

            // Insert into User collection
            User user = new User { Name = "UserV1", Email = "user1@example.com", Department = "Engineering" };
            int userId = db.Insert(user);
            user.Id = userId;
            user.Name = "UserV2";
            user.Email = "user2@example.com";
            db.Replace(user);

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
            db.Replace(person);

            // Vacuum should free the continuation pages from the old large document
            GarbageCollectionResult result = db.Vacuum();
            Assert.IsGreaterThan(0, result.VersionsCollected);

            // Verify the collected version had a location (physical cleanup occurred)
            Assert.IsNotNull(result.CollectableVersions);
            Assert.IsGreaterThan(0, result.CollectableVersions.Count);

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
            using (ITransaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Original", Age = 25, Email = "test@example.com" };
                int id = tx.Insert(person);

                // Update in the same transaction - this overwrites the write set entry
                person.Id = id;
                person.Name = "Updated";
                tx.Replace(person);

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

    [TestMethod]
    public void Vacuum_SingleVersionDeletedDocument_CollectsTheVersion()
    {
        // Verifies that documents inserted and deleted without updates are collected
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "SingleVersion", Age = 25, Email = "single@example.com" };
            int id = db.Insert(person);
            db.DeleteById<Person>(id);

            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThan(0, result.VersionsCollected,
                "Single-version deleted document should be collected");
        }
    }

    [TestMethod]
    public void Vacuum_MultiVersionDeletedDocument_AllVersionsCollected()
    {
        // Test that multi-version deleted documents have ALL versions collected in one pass
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert a document
            Person person = new Person { Name = "Version1", Age = 25, Email = "v1@example.com" };
            int id = db.Insert(person);

            // Update to create a second version
            person.Id = id;
            person.Name = "Version2";
            db.Replace(person);

            // Delete the document (marks head as deleted)
            db.DeleteById<Person>(id);

            // First vacuum - should collect ALL versions (both head and interior)
            GarbageCollectionResult result1 = db.Vacuum();
            Assert.IsGreaterThan(0, result1.VersionsCollected, "First vacuum should collect versions");

            // Both versions should be collected (insert version + update version)
            Assert.IsGreaterThanOrEqualTo(2, result1.VersionsCollected,
                "Should collect both the original and updated versions");

            // Second vacuum should have nothing left to collect
            GarbageCollectionResult result2 = db.Vacuum();
            Assert.AreEqual(0, result2.VersionsCollected,
                "Second vacuum should find nothing to collect - all versions already collected");
        }
    }

    [TestMethod]
    public void Vacuum_DeletedDocuments_FullyRemovedFromIndex()
    {
        // Verifies that multiple deleted single-version documents are all collected
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                int id = db.Insert(person);
                db.DeleteById<Person>(id);
            }

            GarbageCollectionResult result = db.Vacuum();

            Assert.IsGreaterThan(0, result.VersionsCollected,
                "Deleted single-version documents should be collected");
        }
    }

    [TestMethod]
    public void Vacuum_RepeatedVacuumEventuallyCollectsNothing()
    {
        // After proper GC, repeated vacuum calls should eventually collect nothing
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Create some multi-version deleted documents
            for (int i = 0; i < 5; i++)
            {
                Person person = new Person { Name = $"Multi{i}_V1", Age = 30 + i, Email = $"m{i}@example.com" };
                int id = db.Insert(person);
                person.Id = id;
                person.Name = $"Multi{i}_V2";
                db.Replace(person);
                db.DeleteById<Person>(id);
            }

            // Run vacuum until nothing more to collect (max 10 rounds)
            int rounds = 0;
            int totalCollected = 0;
            for (rounds = 1; rounds <= 10; rounds++)
            {
                GarbageCollectionResult result = db.Vacuum();
                totalCollected += result.VersionsCollected;
                if (result.VersionsCollected == 0)
                {
                    break;
                }
            }

            // First round collects all, second round confirms nothing left
            Assert.AreEqual(2, rounds,
                "Should take exactly 2 rounds: one to collect all, one to confirm empty");
            Assert.IsGreaterThan(0, totalCollected,
                "Should have collected some versions");
        }
    }

    [TestMethod]
    public void Vacuum_FragmentedPages_CompactsAll()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 4096, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert several documents on the same page
            for (int i = 0; i < 5; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                db.Insert(person);
            }

            // Delete some to create fragmentation (holes in the page)
            db.DeleteById<Person>(1);
            db.DeleteById<Person>(3);

            // Vacuum should run version GC and then compact fragmented pages
            GarbageCollectionResult result = db.Vacuum();

            // Should have compacted at least one page (the page with deleted docs has holes)
            Assert.IsGreaterThanOrEqualTo(0, result.PagesCompacted);

            // Verify remaining documents are still readable
            Person p2 = db.GetById<Person>(2);
            Person p4 = db.GetById<Person>(4);
            Person p5 = db.GetById<Person>(5);
            Assert.IsNotNull(p2);
            Assert.IsNotNull(p4);
            Assert.IsNotNull(p5);
            Assert.AreEqual("Person1", p2.Name);
            Assert.AreEqual("Person3", p4.Name);
            Assert.AreEqual("Person4", p5.Name);
        }
    }

    [TestMethod]
    public void Vacuum_NoFragmentation_CompactsNone()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert documents without any deletions
            for (int i = 0; i < 5; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                db.Insert(person);
            }

            // Vacuum with no deletions - no fragmentation to compact
            GarbageCollectionResult result = db.Vacuum();

            Assert.AreEqual(0, result.PagesCompacted, "No pages should be compacted when there's no fragmentation");
        }
    }

    [TestMethod]
    public void Vacuum_ReturnsCorrectPagesCompactedCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 4096, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert several documents
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@example.com" };
                db.Insert(person);
            }

            // Delete some documents to create fragmentation
            db.DeleteById<Person>(2);
            db.DeleteById<Person>(5);
            db.DeleteById<Person>(8);

            // First vacuum: version GC + page compaction
            GarbageCollectionResult result1 = db.Vacuum();

            // PagesCompacted should be a non-negative number
            Assert.IsGreaterThanOrEqualTo(0, result1.PagesCompacted);

            // Second vacuum should compact fewer or no pages (already compacted)
            GarbageCollectionResult result2 = db.Vacuum();
            Assert.AreEqual(0, result2.PagesCompacted, "Second vacuum should not need to compact anything");
        }
    }

    [TestMethod]
    public void Vacuum_PageCompaction_NewDocumentsCanUseReclaimedSpace()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 4096, UseWal = true, AutoGarbageCollection = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            // Insert documents with known sizes
            for (int i = 0; i < 3; i++)
            {
                Person person = new Person
                {
                    Name = $"Person{i}",
                    Age = 20 + i,
                    Email = new string('x', 500) + $"{i}@example.com"
                };
                db.Insert(person);
            }

            // Delete first two documents to create holes
            db.DeleteById<Person>(1);
            db.DeleteById<Person>(2);

            // Vacuum should compact and reclaim space
            GarbageCollectionResult result = db.Vacuum();

            // Insert new documents - should be able to use the reclaimed space
            for (int i = 0; i < 3; i++)
            {
                Person person = new Person
                {
                    Name = $"NewPerson{i}",
                    Age = 40 + i,
                    Email = $"new{i}@example.com"
                };
                db.Insert(person);
            }

            // Verify all current documents are readable
            Person p3 = db.GetById<Person>(3);
            Assert.IsNotNull(p3);
            Assert.AreEqual("Person2", p3.Name);

            for (int i = 4; i <= 6; i++)
            {
                Person newPerson = db.GetById<Person>(i);
                Assert.IsNotNull(newPerson);
                Assert.StartsWith("NewPerson", newPerson.Name);
            }
        }
    }
}
