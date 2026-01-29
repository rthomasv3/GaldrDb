using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Json;
using GaldrDbEngine.Query;
using GaldrDbEngine.Query.Execution;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class NestedQueryTests
{
    private List<PersonWithAddress> _testData;
    private InMemoryQueryExecutor<PersonWithAddress> _executor;
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testData = new List<PersonWithAddress>
        {
            new PersonWithAddress
            {
                Id = 1,
                Name = "Alice",
                Address = new Address { Street = "123 Main St", City = "Seattle", State = "WA", ZipCode = 98101 },
                PreviousAddresses = new List<Address>
                {
                    new Address { Street = "456 Oak Ave", City = "Portland", State = "OR", ZipCode = 97201 },
                    new Address { Street = "789 Pine Rd", City = "Denver", State = "CO", ZipCode = 80201 }
                }
            },
            new PersonWithAddress
            {
                Id = 2,
                Name = "Bob",
                Address = new Address { Street = "100 First Ave", City = "Portland", State = "OR", ZipCode = 97201 },
                PreviousAddresses = new List<Address>
                {
                    new Address { Street = "200 Second St", City = "Seattle", State = "WA", ZipCode = 98102 }
                }
            },
            new PersonWithAddress
            {
                Id = 3,
                Name = "Charlie",
                Address = new Address { Street = "300 Third Blvd", City = "Denver", State = "CO", ZipCode = 80202 },
                PreviousAddresses = null
            },
            new PersonWithAddress
            {
                Id = 4,
                Name = "Diana",
                Address = null,
                PreviousAddresses = new List<Address>
                {
                    new Address { Street = "400 Fourth Ln", City = "Chicago", State = "IL", ZipCode = 60601 }
                }
            },
            new PersonWithAddress
            {
                Id = 5,
                Name = "Eve",
                Address = new Address { Street = "500 Fifth Way", City = "Seattle", State = "WA", ZipCode = 98103 },
                PreviousAddresses = new List<Address>()
            }
        };
        _executor = new InMemoryQueryExecutor<PersonWithAddress>(_testData);

        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbNestedQueryTests_{Guid.NewGuid()}");
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

    #region Single Nested Object - Typed Queries

    [TestMethod]
    public void Where_NestedStringEquals_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.City, FieldOp.Equals, "Seattle");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(2, results);
        Assert.IsTrue(results.Exists(p => p.Name == "Alice"));
        Assert.IsTrue(results.Exists(p => p.Name == "Eve"));
    }

    [TestMethod]
    public void Where_NestedIntGreaterThan_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.ZipCode, FieldOp.GreaterThan, 90000);

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(3, results);
    }

    [TestMethod]
    public void Where_NestedFieldNull_DoesNotMatch()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.City, FieldOp.Equals, "Seattle");

        List<PersonWithAddress> results = query.ToList();

        Assert.IsFalse(results.Exists(p => p.Name == "Diana"));
    }

    [TestMethod]
    public void Where_NestedStringStartsWith_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.City, FieldOp.StartsWith, "Port");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Bob", results[0].Name);
    }

    [TestMethod]
    public void Where_MultipleNestedFilters_CombinesWithAnd()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.State, FieldOp.Equals, "WA")
            .Where(PersonWithAddressMeta.Address.ZipCode, FieldOp.GreaterThan, 98101);

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Eve", results[0].Name);
    }

    #endregion

    #region Collection Properties - WhereAny Typed Queries

    [TestMethod]
    public void WhereAny_CollectionFieldEquals_MatchesAnyElement()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Portland");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Alice", results[0].Name);
    }

    [TestMethod]
    public void WhereAny_CollectionFieldEquals_NoMatch_ReturnsEmpty()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Los Angeles");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(0, results);
    }

    [TestMethod]
    public void WhereAny_NullCollection_DoesNotMatch()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Denver");

        List<PersonWithAddress> results = query.ToList();

        Assert.IsFalse(results.Exists(p => p.Name == "Charlie"));
    }

    [TestMethod]
    public void WhereAny_EmptyCollection_DoesNotMatch()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Seattle");

        List<PersonWithAddress> results = query.ToList();

        Assert.IsFalse(results.Exists(p => p.Name == "Eve"));
    }

    [TestMethod]
    public void WhereAny_CollectionFieldGreaterThan_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.ZipCode, FieldOp.GreaterThan, 95000);

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(2, results);
        Assert.IsTrue(results.Exists(p => p.Name == "Alice"));
        Assert.IsTrue(results.Exists(p => p.Name == "Bob"));
    }

    [TestMethod]
    public void WhereAny_CombinedWithNestedWhere_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .Where(PersonWithAddressMeta.Address.State, FieldOp.Equals, "WA")
            .WhereAny(PersonWithAddressMeta.PreviousAddresses.State, FieldOp.Equals, "OR");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Alice", results[0].Name);
    }

    #endregion

    #region Collection Properties - WhereAnyBetween, WhereAnyIn, WhereAnyNotIn

    [TestMethod]
    public void WhereAnyBetween_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAnyBetween(PersonWithAddressMeta.PreviousAddresses.ZipCode, 97000, 98000);

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Alice", results[0].Name);
    }

    [TestMethod]
    public void WhereAnyIn_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAnyIn(PersonWithAddressMeta.PreviousAddresses.City, "Portland", "Denver");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(1, results);
        Assert.AreEqual("Alice", results[0].Name);
    }

    [TestMethod]
    public void WhereAnyNotIn_FiltersCorrectly()
    {
        QueryBuilder<PersonWithAddress> query = new QueryBuilder<PersonWithAddress>(_executor)
            .WhereAnyNotIn(PersonWithAddressMeta.PreviousAddresses.City, "Portland", "Denver");

        List<PersonWithAddress> results = query.ToList();

        Assert.HasCount(2, results);
        Assert.IsTrue(results.Exists(p => p.Name == "Bob"));
        Assert.IsTrue(results.Exists(p => p.Name == "Diana"));
    }

    #endregion

    #region Strongly-Typed Nested Object Queries (Real Database)

    [TestMethod]
    public void TypedDb_Where_NestedPath_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .Where(PersonWithAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public void TypedDb_Where_NestedPathMissing_ReturnsNoMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .Where(PersonWithAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.IsFalse(results.Exists(p => p.Name == "Diana"));
        }
    }

    [TestMethod]
    public void TypedDb_Where_NestedPathGreaterThan_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .Where(PersonWithAddressMeta.Address.ZipCode, FieldOp.GreaterThan, 90000)
                .ToList();

            Assert.HasCount(3, results);
        }
    }

    [TestMethod]
    public void TypedDb_WhereBetween_NestedPath_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereBetween(PersonWithAddressMeta.Address.ZipCode, 97000, 99000)
                .ToList();

            Assert.HasCount(3, results);
        }
    }

    #endregion

    #region Strongly-Typed Collection Queries (Real Database)

    [TestMethod]
    public void TypedDb_WhereAny_CollectionPath_MatchesAnyElement()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Portland")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void TypedDb_WhereAny_NoMatch_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Los Angeles")
                .ToList();

            Assert.HasCount(0, results);
        }
    }

    [TestMethod]
    public void TypedDb_WhereAny_NullCollection_DoesNotMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Denver")
                .ToList();

            Assert.IsFalse(results.Exists(p => p.Name == "Charlie"));
        }
    }

    [TestMethod]
    public void TypedDb_WhereAny_EmptyCollection_DoesNotMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.IsFalse(results.Exists(p => p.Name == "Eve"));
        }
    }

    [TestMethod]
    public void TypedDb_WhereAny_GreaterThan_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.ZipCode, FieldOp.GreaterThan, 95000)
                .ToList();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public void TypedDb_CombinedNestedAndCollection_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<PersonWithAddress> results = db.Query<PersonWithAddress>()
                .Where(PersonWithAddressMeta.Address.State, FieldOp.Equals, "WA")
                .WhereAny(PersonWithAddressMeta.PreviousAddresses.State, FieldOp.Equals, "OR")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    #endregion

    #region Dynamic Nested Object Queries

    [TestMethod]
    public void Dynamic_Where_NestedPath_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .Where("Address.City", FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public void Dynamic_Where_NestedPathMissing_ReturnsNoMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .Where("Address.City", FieldOp.Equals, "Seattle")
                .ToList();

            bool hasDiana = false;
            foreach (JsonDocument doc in results)
            {
                if (doc.GetString("Name") == "Diana")
                {
                    hasDiana = true;
                }
            }
            Assert.IsFalse(hasDiana);
        }
    }

    [TestMethod]
    public void Dynamic_Where_NestedPathGreaterThan_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .Where("Address.ZipCode", FieldOp.GreaterThan, 90000)
                .ToList();

            Assert.HasCount(3, results);
        }
    }

    [TestMethod]
    public void Dynamic_WhereBetween_NestedPath_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereBetween("Address.ZipCode", 97000, 99000)
                .ToList();

            Assert.HasCount(3, results);
        }
    }

    #endregion

    #region Dynamic Collection Queries (WhereAny)

    [TestMethod]
    public void Dynamic_WhereAny_CollectionPath_MatchesAnyElement()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereAny("PreviousAddresses.City", FieldOp.Equals, "Portland")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].GetString("Name"));
        }
    }

    [TestMethod]
    public void Dynamic_WhereAny_NoMatch_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereAny("PreviousAddresses.City", FieldOp.Equals, "Los Angeles")
                .ToList();

            Assert.HasCount(0, results);
        }
    }

    [TestMethod]
    public void Dynamic_WhereAny_NullCollection_DoesNotMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereAny("PreviousAddresses.City", FieldOp.Equals, "Denver")
                .ToList();

            bool hasCharlie = false;
            foreach (JsonDocument doc in results)
            {
                if (doc.GetString("Name") == "Charlie")
                {
                    hasCharlie = true;
                }
            }
            Assert.IsFalse(hasCharlie);
        }
    }

    [TestMethod]
    public void Dynamic_WhereAny_EmptyCollection_DoesNotMatch()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereAny("PreviousAddresses.City", FieldOp.Equals, "Seattle")
                .ToList();

            bool hasEve = false;
            foreach (JsonDocument doc in results)
            {
                if (doc.GetString("Name") == "Eve")
                {
                    hasEve = true;
                }
            }
            Assert.IsFalse(hasEve);
        }
    }

    [TestMethod]
    public void Dynamic_WhereAny_GreaterThan_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .WhereAny("PreviousAddresses.ZipCode", FieldOp.GreaterThan, 95000)
                .ToList();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public void Dynamic_CombinedNestedAndCollection_FiltersCorrectly()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions()))
        {
            foreach (PersonWithAddress person in _testData)
            {
                db.Insert(person);
            }

            List<JsonDocument> results = db.QueryDynamic("PersonWithAddress")
                .Where("Address.State", FieldOp.Equals, "WA")
                .WhereAny("PreviousAddresses.State", FieldOp.Equals, "OR")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].GetString("Name"));
        }
    }

    #endregion
}
