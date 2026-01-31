using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Json;
using GaldrDbEngine.Query;
using GaldrDbEngine.Schema;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class DynamicQueryTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbDynamicQueryTests_{Guid.NewGuid()}");
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

    #region Schema API Tests - GetIndexes

    [TestMethod]
    public void GetIndexes_ExistingCollection_ReturnsIndexInfo()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            IReadOnlyList<IndexInfo> indexes = db.GetIndexes("Person");

            Assert.HasCount(1, indexes);
            Assert.AreEqual("Name", indexes[0].FieldName);
            Assert.AreEqual(GaldrFieldType.String, indexes[0].FieldType);
            Assert.IsFalse(indexes[0].IsUnique);
        }
    }

    [TestMethod]
    public void GetIndexes_NonExistentCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                db.GetIndexes("NonExistent");
            });
        }
    }

    [TestMethod]
    public void GetIndexes_CollectionWithNoIndexes_ReturnsEmptyList()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
            db.DropIndex("Person", "Name");

            IReadOnlyList<IndexInfo> indexes = db.GetIndexes("Person");

            Assert.IsEmpty(indexes);
        }
    }

    #endregion

    #region Schema API Tests - GetCollectionInfo

    [TestMethod]
    public void GetCollectionInfo_ExistingCollection_ReturnsInfo()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            CollectionInfo info = db.GetCollectionInfo("Person");

            Assert.AreEqual("Person", info.Name);
            Assert.AreEqual(2, info.DocumentCount);
            Assert.HasCount(1, info.Indexes);
            Assert.AreEqual("Name", info.Indexes[0].FieldName);
        }
    }

    [TestMethod]
    public void GetCollectionInfo_NonExistentCollection_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Assert.Throws<InvalidOperationException>(() =>
            {
                db.GetCollectionInfo("NonExistent");
            });
        }
    }

    [TestMethod]
    public void GetCollectionInfo_EmptyCollection_ReturnsZeroDocumentCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
            db.DeleteById<Person>(1);

            CollectionInfo info = db.GetCollectionInfo("Person");

            Assert.AreEqual(0, info.DocumentCount);
        }
    }

    #endregion

    #region GetByIdDynamic Tests

    [TestMethod]
    public void GetByIdDynamic_ExistingDocument_ReturnsJsonDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "John Doe", Age = 35, Email = "john@example.com" });

            JsonDocument doc = db.GetByIdDynamic("Person", 1);

            Assert.IsNotNull(doc);
            Assert.AreEqual(1, doc.GetInt32("Id"));
            Assert.AreEqual("John Doe", doc.GetString("Name"));
            Assert.AreEqual(35, doc.GetInt32("Age"));
            Assert.AreEqual("john@example.com", doc.GetString("Email"));
        }
    }

    [TestMethod]
    public void GetByIdDynamic_NonExistentDocument_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            JsonDocument doc = db.GetByIdDynamic("Person", 999);

            Assert.IsNull(doc);
        }
    }

    [TestMethod]
    public async Task GetByIdDynamicAsync_ExistingDocument_ReturnsJsonDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertAsync(new Person { Name = "Async Test", Age = 40, Email = "async@example.com" });

            JsonDocument doc = await db.GetByIdDynamicAsync("Person", 1);

            Assert.IsNotNull(doc);
            Assert.AreEqual("Async Test", doc.GetString("Name"));
            Assert.AreEqual(40, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public async Task GetByIdDynamicAsync_NonExistentDocument_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            JsonDocument doc = await db.GetByIdDynamicAsync("Person", 999);

            Assert.IsNull(doc);
        }
    }

    #endregion

    #region InsertDynamic Tests

    [TestMethod]
    public void InsertDynamic_ValidJson_InsertsAndReturnsId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            string json = "{\"Name\":\"Dynamic Insert\",\"Age\":28,\"Email\":\"dynamic@example.com\"}";
            int id = db.InsertDynamic("Person", json);

            Assert.AreEqual(1, id);

            JsonDocument doc = db.GetByIdDynamic("Person", id);
            Assert.IsNotNull(doc);
            Assert.AreEqual("Dynamic Insert", doc.GetString("Name"));
            Assert.AreEqual(28, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public void InsertDynamic_WithExplicitId_UsesProvidedId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            string json = "{\"Id\":100,\"Name\":\"Explicit Id\",\"Age\":30,\"Email\":\"explicit@example.com\"}";
            int id = db.InsertDynamic("Person", json);

            Assert.AreEqual(100, id);

            JsonDocument doc = db.GetByIdDynamic("Person", 100);
            Assert.IsNotNull(doc);
            Assert.AreEqual("Explicit Id", doc.GetString("Name"));
        }
    }

    [TestMethod]
    public void InsertDynamic_MultipleDocuments_AutoIncrementsIds()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            int id1 = db.InsertDynamic("Person", "{\"Name\":\"First\",\"Age\":20,\"Email\":\"first@example.com\"}");
            int id2 = db.InsertDynamic("Person", "{\"Name\":\"Second\",\"Age\":25,\"Email\":\"second@example.com\"}");
            int id3 = db.InsertDynamic("Person", "{\"Name\":\"Third\",\"Age\":30,\"Email\":\"third@example.com\"}");

            Assert.AreEqual(1, id1);
            Assert.AreEqual(2, id2);
            Assert.AreEqual(3, id3);
        }
    }

    [TestMethod]
    public async Task InsertDynamicAsync_ValidJson_InsertsAndReturnsId()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            string json = "{\"Name\":\"Async Insert\",\"Age\":35,\"Email\":\"async@example.com\"}";
            int id = await db.InsertDynamicAsync("Person", json);

            Assert.AreEqual(1, id);

            JsonDocument doc = await db.GetByIdDynamicAsync("Person", id);
            Assert.IsNotNull(doc);
            Assert.AreEqual("Async Insert", doc.GetString("Name"));
        }
    }

    #endregion

    #region ReplaceDynamic Tests

    [TestMethod]
    public void ReplaceDynamic_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Original\",\"Age\":25,\"Email\":\"original@example.com\"}");

            string updatedJson = "{\"Id\":1,\"Name\":\"Updated\",\"Age\":30,\"Email\":\"updated@example.com\"}";
            bool result = db.ReplaceDynamic("Person", 1, updatedJson);

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual("Updated", doc.GetString("Name"));
            Assert.AreEqual(30, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public void ReplaceDynamic_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            string json = "{\"Id\":999,\"Name\":\"Ghost\",\"Age\":0,\"Email\":\"ghost@example.com\"}";
            bool result = db.ReplaceDynamic("Person", 999, json);

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public async Task ReplaceDynamicAsync_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Original\",\"Age\":25,\"Email\":\"original@example.com\"}");

            string updatedJson = "{\"Id\":1,\"Name\":\"Async Updated\",\"Age\":35,\"Email\":\"async@example.com\"}";
            bool result = await db.ReplaceDynamicAsync("Person", 1, updatedJson);

            Assert.IsTrue(result);

            JsonDocument doc = await db.GetByIdDynamicAsync("Person", 1);
            Assert.AreEqual("Async Updated", doc.GetString("Name"));
        }
    }

    #endregion

    #region DeleteByIdDynamic Tests

    [TestMethod]
    public void DeleteByIdDynamic_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"To Delete\",\"Age\":30,\"Email\":\"delete@example.com\"}");

            bool result = db.DeleteByIdDynamic("Person", 1);

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.IsNull(doc);
        }
    }

    [TestMethod]
    public void DeleteByIdDynamic_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            bool result = db.DeleteByIdDynamic("Person", 999);

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public async Task DeleteByIdDynamicAsync_ExistingDocument_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Async Delete\",\"Age\":25,\"Email\":\"delete@example.com\"}");

            bool result = await db.DeleteByIdDynamicAsync("Person", 1);

            Assert.IsTrue(result);

            JsonDocument doc = await db.GetByIdDynamicAsync("Person", 1);
            Assert.IsNull(doc);
        }
    }

    #endregion

    #region QueryDynamic Tests - Basic

    [TestMethod]
    public void QueryDynamic_ToList_ReturnsAllDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person").ToList();

            Assert.HasCount(3, results);
        }
    }

    [TestMethod]
    public void QueryDynamic_EmptyCollection_ReturnsEmptyList()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
            db.DeleteById<Person>(1);

            List<JsonDocument> results = db.QueryDynamic("Person").ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void QueryDynamic_FirstOrDefault_ReturnsFirstMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            JsonDocument result = db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "Bob")
                .FirstOrDefault();

            Assert.IsNotNull(result);
            Assert.AreEqual("Bob", result.GetString("Name"));
        }
    }

    [TestMethod]
    public void QueryDynamic_FirstOrDefault_NoMatch_ReturnsNull()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            JsonDocument result = db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "NonExistent")
                .FirstOrDefault();

            Assert.IsNull(result);
        }
    }

    [TestMethod]
    public void QueryDynamic_Count_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            int count = db.QueryDynamic("Person").Count();

            Assert.AreEqual(3, count);
        }
    }

    #endregion

    #region QueryDynamic Tests - Filters

    [TestMethod]
    public void QueryDynamic_WhereEquals_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":25,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Where("Age", FieldOp.Equals, 25)
                .ToList();

            Assert.HasCount(2, results);
            foreach (JsonDocument doc in results)
            {
                Assert.AreEqual(25, doc.GetInt32("Age"));
            }
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereGreaterThan_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Where("Age", FieldOp.GreaterThan, 28)
                .ToList();

            Assert.HasCount(2, results);
            foreach (JsonDocument doc in results)
            {
                Assert.IsGreaterThan(28, doc.GetInt32("Age"));
            }
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereLessThan_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Where("Age", FieldOp.LessThan, 32)
                .ToList();

            Assert.HasCount(2, results);
            foreach (JsonDocument doc in results)
            {
                Assert.IsLessThan(32, doc.GetInt32("Age"));
            }
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereStartsWith_OnIndexedField_ReturnsMatches()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Alfred\",\"Age\":30,\"Email\":\"alfred@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":35,\"Email\":\"bob@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Where("Name", FieldOp.StartsWith, "Al")
                .ToList();

            Assert.HasCount(2, results);
            foreach (JsonDocument doc in results)
            {
                Assert.StartsWith("Al", doc.GetString("Name"));
            }
        }
    }

    [TestMethod]
    public void QueryDynamic_MultipleFilters_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Alfred\",\"Age\":35,\"Email\":\"alfred@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":25,\"Email\":\"bob@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Where("Name", FieldOp.StartsWith, "Al")
                .Where("Age", FieldOp.GreaterThan, 30)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alfred", results[0].GetString("Name"));
            Assert.AreEqual(35, results[0].GetInt32("Age"));
        }
    }

    [TestMethod]
    public void QueryDynamic_CountWithFilter_ReturnsFilteredCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            int count = db.QueryDynamic("Person")
                .Where("Age", FieldOp.GreaterThanOrEqual, 30)
                .Count();

            Assert.AreEqual(2, count);
        }
    }

    #endregion

    #region QueryDynamic Tests - WhereBetween, WhereIn, WhereNotIn

    [TestMethod]
    public void QueryDynamic_WhereBetween_ReturnsDocumentsInRange()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":20,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":25,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":30,\"Email\":\"charlie@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Diana\",\"Age\":35,\"Email\":\"diana@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereBetween("Age", 24, 31)
                .ToList();

            Assert.HasCount(2, results);
            foreach (JsonDocument doc in results)
            {
                int age = doc.GetInt32("Age");
                Assert.IsTrue(age >= 24 && age <= 31);
            }
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereBetween_InclusiveOnBothEnds()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereBetween("Age", 25, 35)
                .ToList();

            Assert.HasCount(3, results);
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereIn_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereIn("Name", "Alice", "Charlie")
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.Exists(d => d.GetString("Name") == "Alice"));
            Assert.IsTrue(results.Exists(d => d.GetString("Name") == "Charlie"));
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereIn_WithIntValues_ReturnsMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereIn("Age", 25, 35)
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.Exists(d => d.GetInt32("Age") == 25));
            Assert.IsTrue(results.Exists(d => d.GetInt32("Age") == 35));
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereNotIn_ExcludesMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereNotIn("Name", "Alice", "Charlie")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Bob", results[0].GetString("Name"));
        }
    }

    [TestMethod]
    public void QueryDynamic_WhereNotIn_WithIntValues_ExcludesMatchingDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .WhereNotIn("Age", 25, 35)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual(30, results[0].GetInt32("Age"));
        }
    }

    #endregion

    #region QueryDynamic Tests - Ordering

    [TestMethod]
    public void QueryDynamic_OrderBy_SortsAscending()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .OrderBy("Name")
                .ToList();

            Assert.HasCount(3, results);
            Assert.AreEqual("Alice", results[0].GetString("Name"));
            Assert.AreEqual("Bob", results[1].GetString("Name"));
            Assert.AreEqual("Charlie", results[2].GetString("Name"));
        }
    }

    [TestMethod]
    public void QueryDynamic_OrderByDescending_SortsDescending()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            List<JsonDocument> results = db.QueryDynamic("Person")
                .OrderByDescending("Age")
                .ToList();

            Assert.HasCount(3, results);
            Assert.AreEqual(35, results[0].GetInt32("Age"));
            Assert.AreEqual(30, results[1].GetInt32("Age"));
            Assert.AreEqual(25, results[2].GetInt32("Age"));
        }
    }

    #endregion

    #region QueryDynamic Tests - Pagination

    [TestMethod]
    public void QueryDynamic_WithLimit_ReturnsLimitedResults()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 1; i <= 10; i++)
            {
                db.InsertDynamic("Person", $"{{\"Name\":\"Person{i}\",\"Age\":{20 + i},\"Email\":\"person{i}@example.com\"}}");
            }

            List<JsonDocument> results = db.QueryDynamic("Person")
                .Limit(5)
                .ToList();

            Assert.HasCount(5, results);
        }
    }

    [TestMethod]
    public void QueryDynamic_WithSkip_SkipsResults()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 1; i <= 10; i++)
            {
                db.InsertDynamic("Person", $"{{\"Name\":\"Person{i}\",\"Age\":{20 + i},\"Email\":\"person{i}@example.com\"}}");
            }

            List<JsonDocument> allResults = db.QueryDynamic("Person").ToList();
            List<JsonDocument> skippedResults = db.QueryDynamic("Person").Skip(3).ToList();

            Assert.HasCount(allResults.Count - 3, skippedResults);
        }
    }

    [TestMethod]
    public void QueryDynamic_WithSkipAndLimit_Paginates()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 1; i <= 10; i++)
            {
                db.InsertDynamic("Person", $"{{\"Name\":\"Person{i:D2}\",\"Age\":{20 + i},\"Email\":\"person{i}@example.com\"}}");
            }

            List<JsonDocument> page1 = db.QueryDynamic("Person")
                .OrderBy("Name")
                .Skip(0)
                .Limit(3)
                .ToList();

            List<JsonDocument> page2 = db.QueryDynamic("Person")
                .OrderBy("Name")
                .Skip(3)
                .Limit(3)
                .ToList();

            Assert.HasCount(3, page1);
            Assert.HasCount(3, page2);

            HashSet<int> page1Ids = new HashSet<int>();
            foreach (JsonDocument doc in page1)
            {
                page1Ids.Add(doc.GetInt32("Id"));
            }

            foreach (JsonDocument doc in page2)
            {
                Assert.DoesNotContain(doc.GetInt32("Id"), page1Ids, "Page 1 and Page 2 should not overlap");
            }
        }
    }

    #endregion

    #region QueryDynamic Tests - Async

    [TestMethod]
    public async Task QueryDynamic_ToListAsync_ReturnsAllDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            List<JsonDocument> results = await db.QueryDynamic("Person").ToListAsync();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public async Task QueryDynamic_FirstOrDefaultAsync_ReturnsFirstMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            JsonDocument result = await db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "Bob")
                .FirstOrDefaultAsync();

            Assert.IsNotNull(result);
            Assert.AreEqual("Bob", result.GetString("Name"));
        }
    }

    [TestMethod]
    public async Task QueryDynamic_CountAsync_ReturnsCorrectCount()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            await db.InsertDynamicAsync("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            int count = await db.QueryDynamic("Person").CountAsync();

            Assert.AreEqual(3, count);
        }
    }

    #endregion

    #region Transaction Dynamic Query Tests

    [TestMethod]
    public void Transaction_GetByIdDynamic_ReturnsDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                JsonDocument doc = tx.GetByIdDynamic("Person", 1);

                Assert.IsNotNull(doc);
                Assert.AreEqual("Test", doc.GetString("Name"));
            }
        }
    }

    [TestMethod]
    public void Transaction_InsertDynamic_CanBeQueriedBeforeCommit()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                int id = tx.InsertDynamic("Person", "{\"Name\":\"Uncommitted\",\"Age\":30,\"Email\":\"uncommitted@example.com\"}");

                JsonDocument doc = tx.GetByIdDynamic("Person", id);

                Assert.IsNotNull(doc);
                Assert.AreEqual("Uncommitted", doc.GetString("Name"));
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_IncludesWriteSetInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Committed\",\"Age\":25,\"Email\":\"committed@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.InsertDynamic("Person", "{\"Name\":\"Uncommitted\",\"Age\":30,\"Email\":\"uncommitted@example.com\"}");

                List<JsonDocument> results = tx.QueryDynamic("Person").ToList();

                Assert.HasCount(2, results);
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_ExcludesDeletedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.DeleteByIdDynamic("Person", 1);

                List<JsonDocument> results = tx.QueryDynamic("Person").ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("Bob", results[0].GetString("Name"));
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_ReflectsUpdatedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Original\",\"Age\":25,\"Email\":\"original@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.ReplaceDynamic("Person", 1, "{\"Id\":1,\"Name\":\"Updated\",\"Age\":30,\"Email\":\"updated@example.com\"}");

                JsonDocument doc = tx.QueryDynamic("Person")
                    .Where("Name", FieldOp.Equals, "Updated")
                    .FirstOrDefault();

                Assert.IsNotNull(doc);
                Assert.AreEqual("Updated", doc.GetString("Name"));
                Assert.AreEqual(30, doc.GetInt32("Age"));
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_CountIncludesUncommittedInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");
                tx.InsertDynamic("Person", "{\"Name\":\"Diana\",\"Age\":28,\"Email\":\"diana@example.com\"}");

                int count = tx.QueryDynamic("Person").Count();

                Assert.AreEqual(4, count);
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_CountExcludesDeletedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":30,\"Email\":\"bob@example.com\"}");
            db.InsertDynamic("Person", "{\"Name\":\"Charlie\",\"Age\":35,\"Email\":\"charlie@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.DeleteByIdDynamic("Person", 1);

                int count = tx.QueryDynamic("Person").Count();

                Assert.AreEqual(2, count);
            }
        }
    }

    [TestMethod]
    public void Transaction_CommittedInsertDynamic_VisibleAfterCommit()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.InsertDynamic("Person", "{\"Name\":\"Committed\",\"Age\":25,\"Email\":\"committed@example.com\"}");
                tx.Commit();
            }

            JsonDocument doc = db.GetByIdDynamic("Person", 1);

            Assert.IsNotNull(doc);
            Assert.AreEqual("Committed", doc.GetString("Name"));
        }
    }

    [TestMethod]
    public void Transaction_RolledBackInsertDynamic_NotVisibleAfterRollback()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.InsertDynamic("Person", "{\"Name\":\"RolledBack\",\"Age\":25,\"Email\":\"rolledback@example.com\"}");
            }

            List<JsonDocument> results = db.QueryDynamic("Person").ToList();

            Assert.IsEmpty(results);
        }
    }

    #endregion

    #region Full Dynamic CRUD Cycle Tests

    [TestMethod]
    public void FullDynamicCRUDCycle_InsertGetUpdateDelete()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            int id = db.InsertDynamic("Person", "{\"Name\":\"Full Cycle\",\"Age\":25,\"Email\":\"cycle@example.com\"}");
            Assert.AreEqual(1, id);

            JsonDocument retrieved = db.GetByIdDynamic("Person", id);
            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Full Cycle", retrieved.GetString("Name"));

            bool updated = db.ReplaceDynamic("Person", id, "{\"Id\":1,\"Name\":\"Updated Cycle\",\"Age\":30,\"Email\":\"updated@example.com\"}");
            Assert.IsTrue(updated);

            JsonDocument afterUpdate = db.GetByIdDynamic("Person", id);
            Assert.AreEqual("Updated Cycle", afterUpdate.GetString("Name"));
            Assert.AreEqual(30, afterUpdate.GetInt32("Age"));

            bool deleted = db.DeleteByIdDynamic("Person", id);
            Assert.IsTrue(deleted);

            JsonDocument afterDelete = db.GetByIdDynamic("Person", id);
            Assert.IsNull(afterDelete);
        }
    }

    [TestMethod]
    public void DynamicAndTypedApi_Interoperable()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Typed Insert", Age = 25, Email = "typed@example.com" });

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.IsNotNull(doc);
            Assert.AreEqual("Typed Insert", doc.GetString("Name"));

            db.InsertDynamic("Person", "{\"Name\":\"Dynamic Insert\",\"Age\":30,\"Email\":\"dynamic@example.com\"}");

            Person person = db.GetById<Person>(2);
            Assert.IsNotNull(person);
            Assert.AreEqual("Dynamic Insert", person.Name);

            List<JsonDocument> dynamicResults = db.QueryDynamic("Person").ToList();
            Assert.HasCount(2, dynamicResults);

            List<Person> typedResults = db.Query<Person>().ToList();
            Assert.HasCount(2, typedResults);
        }
    }

    [TestMethod]
    public void DynamicQuery_Explain_ReturnsQueryExplanation()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "Test")
                .Explain();

            Assert.IsNotNull(explanation);
            Assert.IsNotNull(explanation.ScanDescription);
        }
    }

    #endregion

    #region UpdateByIdDynamic Tests

    [TestMethod]
    public void UpdateByIdDynamic_UpdatesSingleField()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            bool result = db.UpdateByIdDynamic("Person", 1)
                .Set("Age", 30)
                .Execute();

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual(30, doc.GetInt32("Age"));
            Assert.AreEqual("Alice", doc.GetString("Name"));
        }
    }

    [TestMethod]
    public void UpdateByIdDynamic_UpdatesMultipleFields()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            bool result = db.UpdateByIdDynamic("Person", 1)
                .Set("Name", "Alice Smith")
                .Set("Age", 30)
                .Set("Email", "alice.smith@example.com")
                .Execute();

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual("Alice Smith", doc.GetString("Name"));
            Assert.AreEqual(30, doc.GetInt32("Age"));
            Assert.AreEqual("alice.smith@example.com", doc.GetString("Email"));
        }
    }

    [TestMethod]
    public void UpdateByIdDynamic_NonExistentDocument_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            bool result = db.UpdateByIdDynamic("Person", 999)
                .Set("Age", 30)
                .Execute();

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void UpdateByIdDynamic_UpdatesIndexedField()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            bool result = db.UpdateByIdDynamic("Person", 1)
                .Set("Name", "Bob")
                .Execute();

            Assert.IsTrue(result);

            JsonDocument doc = db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "Bob")
                .FirstOrDefault();

            Assert.IsNotNull(doc);
            Assert.AreEqual(1, doc.GetInt32("Id"));
        }
    }

    [TestMethod]
    public async Task UpdateByIdDynamic_ExecuteAsync_UpdatesDocument()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            bool result = await db.UpdateByIdDynamic("Person", 1)
                .Set("Age", 35)
                .ExecuteAsync();

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual(35, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public void Transaction_UpdateByIdDynamic_VisibleWithinTransaction()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                bool result = tx.UpdateByIdDynamic("Person", 1)
                    .Set("Age", 30)
                    .Execute();

                Assert.IsTrue(result);

                JsonDocument doc = tx.GetByIdDynamic("Person", 1);
                Assert.AreEqual(30, doc.GetInt32("Age"));
            }
        }
    }

    [TestMethod]
    public void Transaction_UpdateByIdDynamic_CommittedChangesVisible()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateByIdDynamic("Person", 1)
                    .Set("Age", 30)
                    .Execute();

                tx.Commit();
            }

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual(30, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public void Transaction_UpdateByIdDynamic_RolledBackChangesNotVisible()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateByIdDynamic("Person", 1)
                    .Set("Age", 30)
                    .Execute();
            }

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual(25, doc.GetInt32("Age"));
        }
    }

    [TestMethod]
    public void UpdateByIdDynamic_SupportsDifferentTypes()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.InsertDynamic("Person", "{\"Name\":\"Alice\",\"Age\":25,\"Email\":\"alice@example.com\"}");

            DateTime now = DateTime.UtcNow;
            Guid guid = Guid.NewGuid();

            bool result = db.UpdateByIdDynamic("Person", 1)
                .Set("Name", "Updated")
                .Set("Age", 30)
                .Set("Score", 99.5)
                .Set("Balance", 1000.50m)
                .Set("IsActive", true)
                .Set("Created", now)
                .Set("UniqueId", guid)
                .Execute();

            Assert.IsTrue(result);

            JsonDocument doc = db.GetByIdDynamic("Person", 1);
            Assert.AreEqual("Updated", doc.GetString("Name"));
            Assert.AreEqual(30, doc.GetInt32("Age"));
            Assert.AreEqual(99.5, doc.GetDouble("Score"));
            Assert.AreEqual(1000.50m, doc.GetDecimal("Balance"));
            Assert.IsTrue(doc.GetBoolean("IsActive"));
            Assert.AreEqual(guid, doc.GetGuid("UniqueId"));
        }
    }

    #endregion

    #region QueryDynamic Tests - Any

    [TestMethod]
    public void QueryDynamic_Any_WithMatches_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@test.com" });

            bool result = db.QueryDynamic("Person")
                .Where("Age", FieldOp.GreaterThan, 28)
                .Any();

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void QueryDynamic_Any_WithNoMatches_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@example.com" });

            bool result = db.QueryDynamic("Person")
                .Where("Age", FieldOp.GreaterThan, 100)
                .Any();

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void QueryDynamic_Any_EmptyCollection_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
            db.DeleteById<Person>(1);

            bool result = db.QueryDynamic("Person").Any();

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void QueryDynamic_Any_WithNoFilters_ReturnsTrueWhenDataExists()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            bool result = db.QueryDynamic("Person").Any();

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void QueryDynamic_Any_MultipleFilters_WorksCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@test.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            bool result = db.QueryDynamic("Person")
                .Where("Age", FieldOp.GreaterThanOrEqual, 30)
                .Where("Email", FieldOp.EndsWith, "@test.com")
                .Any();

            Assert.IsTrue(result); // Bob matches
        }
    }

    [TestMethod]
    public async Task QueryDynamic_AnyAsync_WithMatches_ReturnsTrue()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            bool result = await db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "Alice")
                .AnyAsync();

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public async Task QueryDynamic_AnyAsync_WithNoMatches_ReturnsFalse()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            bool result = await db.QueryDynamic("Person")
                .Where("Name", FieldOp.Equals, "NonExistent")
                .AnyAsync();

            Assert.IsFalse(result);
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_Any_IncludesWriteSetInserts()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.InsertDynamic("Person", "{\"Name\":\"Bob\",\"Age\":50,\"Email\":\"bob@example.com\"}");

                bool result = tx.QueryDynamic("Person")
                    .Where("Age", FieldOp.GreaterThan, 40)
                    .Any();

                Assert.IsTrue(result); // Uncommitted insert should be visible
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_Any_ExcludesDeletedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(1);

                bool result = tx.QueryDynamic("Person").Any();

                Assert.IsFalse(result); // Deleted document should not be counted
            }
        }
    }

    [TestMethod]
    public void Transaction_QueryDynamic_Any_ReflectsUpdatedDocuments()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@example.com" });

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateByIdDynamic("Person", 1)
                    .Set("Age", 50)
                    .Execute();

                bool beforeUpdate = tx.QueryDynamic("Person")
                    .Where("Age", FieldOp.Equals, 25)
                    .Any();

                bool afterUpdate = tx.QueryDynamic("Person")
                    .Where("Age", FieldOp.Equals, 50)
                    .Any();

                Assert.IsFalse(beforeUpdate); // Old value should not match
                Assert.IsTrue(afterUpdate);    // New value should match
            }
        }
    }

    #endregion
}
