using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class IndexedQueryTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbIndexedQueryTests_{Guid.NewGuid()}");
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

    private GaldrDbInstance CreateDatabaseWithPersons(string dbName, int count)
    {
        string dbPath = Path.Combine(_testDirectory, dbName);
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options);

        for (int i = 0; i < count; i++)
        {
            Person person = new Person
            {
                Name = $"Person{i:D3}",
                Age = 20 + (i % 50),
                Email = $"person{i}@example.com"
            };
            db.Insert(person);
        }

        return db;
    }

    [TestMethod]
    public void Query_WithEqualsOnIndexedField_UsesIndex()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("equals_test.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Person050")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Person050", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_WithEqualsOnIndexedField_NoMatch_ReturnsEmpty()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("equals_nomatch.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent")
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_WithStartsWithOnIndexedField_ReturnsMatches()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("startswith_test.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .ToList();

            Assert.HasCount(10, results);
            foreach (Person p in results)
            {
                Assert.StartsWith("Person00", p.Name);
            }
        }
    }

    [TestMethod]
    public void Query_WithMultipleFilters_IndexedAndNonIndexed()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("multi_filter.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person0")
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 25)
                .ToList();

            foreach (Person p in results)
            {
                Assert.StartsWith("Person0", p.Name);
                Assert.IsGreaterThan(25, p.Age);
            }
        }
    }

    [TestMethod]
    public void Query_WithLimit_ReturnsLimitedResults()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("limit_test.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person")
                .Limit(5)
                .ToList();

            Assert.HasCount(5, results);
        }
    }

    [TestMethod]
    public void Query_WithSkip_SkipsResults()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("skip_test.db", 100))
        {
            List<Person> allResults = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .ToList();

            List<Person> skippedResults = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .Skip(3)
                .ToList();

            Assert.HasCount(allResults.Count - 3, skippedResults);
        }
    }

    [TestMethod]
    public void Query_WithSkipAndLimit_Paginates()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("paginate_test.db", 100))
        {
            List<Person> page1 = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person0")
                .Skip(0)
                .Limit(5)
                .ToList();

            List<Person> page2 = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person0")
                .Skip(5)
                .Limit(5)
                .ToList();

            Assert.HasCount(5, page1);
            Assert.HasCount(5, page2);

            foreach (Person p in page1)
            {
                bool foundInPage2 = false;
                foreach (Person p2 in page2)
                {
                    if (p.Id == p2.Id)
                    {
                        foundInPage2 = true;
                        break;
                    }
                }
                Assert.IsFalse(foundInPage2, "Page 1 and Page 2 should not overlap");
            }
        }
    }

    [TestMethod]
    public void Query_Count_WithIndexedFilter_ReturnsCorrectCount()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("count_test.db", 100))
        {
            int count = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .Count();

            Assert.AreEqual(10, count);
        }
    }

    [TestMethod]
    public void Query_Count_WithNoRemainingFilters_DoesNotDeserialize()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("count_opt.db", 100))
        {
            int count = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Person050")
                .Count();

            Assert.AreEqual(1, count);
        }
    }

    [TestMethod]
    public void Query_FirstOrDefault_WithIndexedFilter_ReturnsFirst()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("first_test.db", 100))
        {
            Person result = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Person050")
                .FirstOrDefault();

            Assert.IsNotNull(result);
            Assert.AreEqual("Person050", result.Name);
        }
    }

    [TestMethod]
    public void Query_FirstOrDefault_NoMatch_ReturnsNull()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("first_null.db", 100))
        {
            Person result = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent")
                .FirstOrDefault();

            Assert.IsNull(result);
        }
    }

    [TestMethod]
    public void Query_WithNoFilters_ReturnsAll()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("no_filter.db", 50))
        {
            List<Person> results = db.Query<Person>()
                .ToList();

            Assert.HasCount(50, results);
        }
    }

    [TestMethod]
    public void Query_WithNonIndexedFilter_UsesFullScan()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("nonindexed.db", 100))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.Equals, 25)
                .ToList();

            foreach (Person p in results)
            {
                Assert.AreEqual(25, p.Age);
            }
        }
    }

    [TestMethod]
    public void QueryPlanner_ChoosesEqualsOverStartsWith()
    {
        string dbPath = Path.Combine(_testDirectory, "planner_priority.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                Person person = new Person { Name = $"Test{i}", Age = 20 + i, Email = $"test{i}@example.com" };
                db.Insert(person);
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Test")
                .Where(PersonMeta.Name, FieldOp.Equals, "Test5")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Test5", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_EmptyCollection_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "empty.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Test")
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_NonExistentCollection_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "no_collection.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Test")
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_MultipleEqualsMatches_ReturnsAll()
    {
        string dbPath = Path.Combine(_testDirectory, "multi_match.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 5; i++)
            {
                Person person = new Person { Name = "SameName", Age = 20 + i, Email = $"person{i}@example.com" };
                db.Insert(person);
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "SameName")
                .ToList();

            Assert.HasCount(5, results);
        }
    }
}
