using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class UniqueConstraintTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbUniqueConstraintTests_{Guid.NewGuid()}");
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
    public void Insert_WithUniqueField_FirstInsert_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "unique_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id = db.Insert(user);

            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void Insert_WithDuplicateUniqueField_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "unique_dup_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user1 = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            db.Insert(user1);

            User user2 = new User { Name = "Jane", Email = "john@example.com", Department = "Marketing" };

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Insert(user2);
            });

            Assert.Contains("Unique constraint violation", exception.Message);
            Assert.Contains("Email", exception.Message);
            Assert.Contains("john@example.com", exception.Message);
        }
    }

    [TestMethod]
    public void Insert_WithDifferentUniqueValues_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "unique_diff_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user1 = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id1 = db.Insert(user1);

            User user2 = new User { Name = "Jane", Email = "jane@example.com", Department = "Engineering" };
            int id2 = db.Insert(user2);

            Assert.AreEqual(1, id1);
            Assert.AreEqual(2, id2);
        }
    }

    [TestMethod]
    public void Insert_WithDuplicateNonUniqueField_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "nonunique_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user1 = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id1 = db.Insert(user1);

            User user2 = new User { Name = "Jane", Email = "jane@example.com", Department = "Engineering" };
            int id2 = db.Insert(user2);

            Assert.AreEqual(1, id1);
            Assert.AreEqual(2, id2);
        }
    }

    [TestMethod]
    public void Update_WithSameUniqueValue_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "update_same_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id = db.Insert(user);

            user.Name = "John Updated";
            bool updated = db.Update(user);

            Assert.IsTrue(updated);

            User retrieved = db.GetById<User>(id);
            Assert.AreEqual("John Updated", retrieved.Name);
            Assert.AreEqual("john@example.com", retrieved.Email);
        }
    }

    [TestMethod]
    public void Update_ChangingToExistingUniqueValue_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "update_dup_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user1 = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            db.Insert(user1);

            User user2 = new User { Name = "Jane", Email = "jane@example.com", Department = "Marketing" };
            db.Insert(user2);

            user2.Email = "john@example.com";

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Update(user2);
            });

            Assert.Contains("Unique constraint violation", exception.Message);
        }
    }

    [TestMethod]
    public void Update_ChangingToNewUniqueValue_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "update_new_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id = db.Insert(user);

            user.Email = "john.doe@example.com";
            bool updated = db.Update(user);

            Assert.IsTrue(updated);

            User retrieved = db.GetById<User>(id);
            Assert.AreEqual("john.doe@example.com", retrieved.Email);
        }
    }

    [TestMethod]
    public void Delete_ThenInsertWithSameUniqueValue_Succeeds()
    {
        string dbPath = Path.Combine(_testDirectory, "delete_reuse_test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            User user1 = new User { Name = "John", Email = "john@example.com", Department = "Engineering" };
            int id1 = db.Insert(user1);

            db.Delete<User>(id1);

            User user2 = new User { Name = "New John", Email = "john@example.com", Department = "Marketing" };
            int id2 = db.Insert(user2);

            Assert.AreEqual(2, id2);

            User retrieved = db.GetById<User>(id2);
            Assert.AreEqual("New John", retrieved.Name);
            Assert.AreEqual("john@example.com", retrieved.Email);
        }
    }

    [TestMethod]
    public void UniqueIndexFieldNames_IsPopulatedCorrectly()
    {
        Assert.IsNotEmpty(UserMeta.UniqueIndexFieldNames);
        Assert.AreEqual("Email", UserMeta.UniqueIndexFieldNames[0]);
    }

    [TestMethod]
    public void IndexedFieldNames_IncludesBothUniqueAndNonUnique()
    {
        Assert.IsGreaterThanOrEqualTo(2, UserMeta.IndexedFieldNames.Count);
    }
}
