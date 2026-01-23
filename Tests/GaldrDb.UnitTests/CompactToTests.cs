using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GaldrDbEngine;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class CompactToTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "GaldrDbCompactTests", Guid.NewGuid().ToString());
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
    public void CompactTo_BasicCompaction_CopiesAllDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 10; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(1, result.CollectionsCompacted);
            Assert.AreEqual(10, result.DocumentsCopied);
            Assert.IsGreaterThan(0, result.TargetFileSize);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            List<Person> people = targetDb.Query<Person>().ToList();
            Assert.HasCount(10, people);
        }
    }

    [TestMethod]
    public void CompactTo_PreservesDocumentIds()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
                tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
                tx.Commit();
            }

            db.CompactTo(targetPath);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            Person alice = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 1).FirstOrDefault();
            Person bob = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 2).FirstOrDefault();
            Person charlie = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 3).FirstOrDefault();

            Assert.IsNotNull(alice);
            Assert.AreEqual("Alice", alice.Name);
            Assert.IsNotNull(bob);
            Assert.AreEqual("Bob", bob.Name);
            Assert.IsNotNull(charlie);
            Assert.AreEqual("Charlie", charlie.Name);
        }
    }

    [TestMethod]
    public void CompactTo_PreservesNextId()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
                tx.Commit();
            }

            db.CompactTo(targetPath);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            using (Transaction tx = targetDb.BeginTransaction())
            {
                int newId = tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
                tx.Commit();

                Assert.AreEqual(3, newId);
            }
        }
    }

    [TestMethod]
    public void CompactTo_ExcludesDeletedDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
                tx.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
                tx.Commit();
            }

            using (Transaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(2);
                tx.Commit();
            }

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(2, result.DocumentsCopied);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            List<Person> people = targetDb.Query<Person>().ToList();
            Assert.HasCount(2, people);

            Person bob = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 2).FirstOrDefault();
            Assert.IsNull(bob);
        }
    }

    [TestMethod]
    public void CompactTo_ThrowsIfTransactionActive()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Commit();
            }

            using (Transaction openTx = db.BeginTransaction())
            {
                Assert.Throws<InvalidOperationException>(() => db.CompactTo(targetPath));
            }
        }
    }

    [TestMethod]
    public void CompactTo_WithSecondaryIndex_PreservesIndex()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new User { Name = "Alice", Email = "alice@example.com", Department = "Engineering" });
                tx.Insert(new User { Name = "Bob", Email = "bob@example.com", Department = "Engineering" });
                tx.Insert(new User { Name = "Charlie", Email = "charlie@example.com", Department = "Sales" });
                tx.Commit();
            }

            db.CompactTo(targetPath);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            User bob = targetDb.Query<User>()
                .Where(UserMeta.Email, FieldOp.Equals, "bob@example.com")
                .FirstOrDefault();

            Assert.IsNotNull(bob);
            Assert.AreEqual("Bob", bob.Name);
        }
    }

    [TestMethod]
    public void CompactTo_MultipleCollections_CopiesAll()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
                tx.Insert(new User { Name = "Admin1", Email = "admin1@example.com", Department = "IT" });
                tx.Insert(new User { Name = "Admin2", Email = "admin2@example.com", Department = "IT" });
                tx.Commit();
            }

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(2, result.CollectionsCompacted);
            Assert.AreEqual(4, result.DocumentsCopied);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            List<Person> people = targetDb.Query<Person>().ToList();
            List<User> users = targetDb.Query<User>().ToList();

            Assert.HasCount(2, people);
            Assert.HasCount(2, users);
        }
    }

    [TestMethod]
    public void CompactTo_ReducesFileSizeAfterDeletes()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        // Use smaller expansion to make test faster while still exercising multiple expansions
        GaldrDbOptions options = new GaldrDbOptions { ExpansionPageCount = 32 };
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, options))
        {
            // Insert enough documents to cause multiple expansions
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 2000; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i} with a longer name to take up more space in the database file", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            // Delete most documents
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 1900; i++)
                {
                    tx.DeleteById<Person>(i);
                }
                tx.Commit();
            }

            long sourceSize = new FileInfo(sourcePath).Length;
            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(100, result.DocumentsCopied);
            Assert.IsLessThan(sourceSize, result.TargetFileSize, $"Target size {result.TargetFileSize} should be less than source size {sourceSize}");
            Assert.IsGreaterThan(0, result.BytesSaved);
        }
    }

    [TestMethod]
    public async Task CompactToAsync_CopiesAllDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 10; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            DatabaseCompactResult result = await db.CompactToAsync(targetPath);

            Assert.AreEqual(1, result.CollectionsCompacted);
            Assert.AreEqual(10, result.DocumentsCopied);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            List<Person> people = targetDb.Query<Person>().ToList();
            Assert.HasCount(10, people);
        }
    }

    [TestMethod]
    public void CompactTo_EmptyDatabase_CreatesValidEmptyDatabase()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(0, result.CollectionsCompacted);
            Assert.AreEqual(0, result.DocumentsCopied);
        }

        using (GaldrDbEngine.GaldrDb targetDb = GaldrDbEngine.GaldrDb.Open(targetPath))
        {
            using (Transaction tx = targetDb.BeginTransaction())
            {
                int id = tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Commit();
                Assert.AreEqual(1, id);
            }
        }
    }

    [TestMethod]
    public void CompactTo_TargetFileAlreadyExists_ThrowsException()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");

        File.WriteAllText(targetPath, "dummy");

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(sourcePath, new GaldrDbOptions()))
        {
            Assert.Throws<InvalidOperationException>(() => db.CompactTo(targetPath));
        }
    }
}
