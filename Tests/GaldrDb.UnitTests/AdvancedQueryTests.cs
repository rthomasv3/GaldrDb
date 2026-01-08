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
public class AdvancedQueryTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbAdvancedQueryTests_{Guid.NewGuid()}");
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

    #region NotIn Tests

    [TestMethod]
    public void WhereNotIn_ExcludesSpecifiedValues()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("notin_test.db", 10))
        {
            List<Person> results = db.Query<Person>()
                .WhereNotIn(PersonMeta.Name, "Person000", "Person001", "Person002")
                .ToList();

            Assert.HasCount(7, results);
            foreach (Person p in results)
            {
                Assert.AreNotEqual("Person000", p.Name);
                Assert.AreNotEqual("Person001", p.Name);
                Assert.AreNotEqual("Person002", p.Name);
            }
        }
    }

    [TestMethod]
    public void WhereNotIn_WithEmptyExclusionList_ReturnsAll()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("notin_empty.db", 10))
        {
            List<Person> results = db.Query<Person>()
                .WhereNotIn(PersonMeta.Name, new string[] { })
                .ToList();

            Assert.HasCount(10, results);
        }
    }

    [TestMethod]
    public void WhereNotIn_WithAllValuesExcluded_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "notin_all.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "alice@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "bob@test.com" });

            List<Person> results = db.Query<Person>()
                .WhereNotIn(PersonMeta.Name, "Alice", "Bob")
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void WhereNotIn_CombinedWithOtherFilters()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("notin_combined.db", 20))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 25)
                .WhereNotIn(PersonMeta.Name, "Person006", "Person007")
                .ToList();

            foreach (Person p in results)
            {
                Assert.IsGreaterThan(25, p.Age);
                Assert.AreNotEqual("Person006", p.Name);
                Assert.AreNotEqual("Person007", p.Name);
            }
        }
    }

    #endregion

    #region Validation Tests

    [TestMethod]
    public void Where_StartsWithOnNonStringField_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_startswith.db", 1))
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(() =>
            {
                db.Query<Person>()
                  .Where(PersonMeta.Age, FieldOp.StartsWith, 25)
                  .ToList();
            });

            Assert.Contains("StartsWith", exception.Message);
            Assert.Contains("string", exception.Message);
        }
    }

    [TestMethod]
    public void Where_EndsWithOnNonStringField_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_endswith.db", 1))
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(() =>
            {
                db.Query<Person>()
                  .Where(PersonMeta.Age, FieldOp.EndsWith, 25)
                  .ToList();
            });

            Assert.Contains("EndsWith", exception.Message);
            Assert.Contains("string", exception.Message);
        }
    }

    [TestMethod]
    public void Where_ContainsOnNonStringField_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_contains.db", 1))
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(() =>
            {
                db.Query<Person>()
                  .Where(PersonMeta.Age, FieldOp.Contains, 25)
                  .ToList();
            });

            Assert.Contains("Contains", exception.Message);
            Assert.Contains("string", exception.Message);
        }
    }

    [TestMethod]
    public void Where_BetweenOperation_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_between.db", 1))
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(() =>
            {
                db.Query<Person>()
                  .Where(PersonMeta.Age, FieldOp.Between, 25)
                  .ToList();
            });

            Assert.Contains("Between", exception.Message);
            Assert.Contains("WhereBetween", exception.Message);
        }
    }

    [TestMethod]
    public void Where_InOperation_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_in.db", 1))
        {
            ArgumentException exception = Assert.ThrowsExactly<ArgumentException>(() =>
            {
                db.Query<Person>()
                  .Where(PersonMeta.Age, FieldOp.In, 25)
                  .ToList();
            });

            Assert.Contains("In", exception.Message);
            Assert.Contains("WhereIn", exception.Message);
        }
    }

    [TestMethod]
    public void Where_StringOperationsOnStringField_Works()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("validate_string_ok.db", 10))
        {
            List<Person> startsWithResults = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .ToList();

            List<Person> endsWithResults = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.EndsWith, "009")
                .ToList();

            List<Person> containsResults = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Contains, "son00")
                .ToList();

            Assert.HasCount(10, startsWithResults);
            Assert.HasCount(1, endsWithResults);
            Assert.HasCount(10, containsResults);
        }
    }

    #endregion

    #region OrderBy Tests

    [TestMethod]
    public void OrderBy_SortsAscending()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_asc.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Charlie", Age = 30, Email = "c@test.com" });
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "a@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 35, Email = "b@test.com" });

            List<Person> results = db.Query<Person>()
                .OrderBy(PersonMeta.Name)
                .ToList();

            Assert.HasCount(3, results);
            Assert.AreEqual("Alice", results[0].Name);
            Assert.AreEqual("Bob", results[1].Name);
            Assert.AreEqual("Charlie", results[2].Name);
        }
    }

    [TestMethod]
    public void OrderByDescending_SortsDescending()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_desc.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Charlie", Age = 30, Email = "c@test.com" });
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "a@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 35, Email = "b@test.com" });

            List<Person> results = db.Query<Person>()
                .OrderByDescending(PersonMeta.Name)
                .ToList();

            Assert.HasCount(3, results);
            Assert.AreEqual("Charlie", results[0].Name);
            Assert.AreEqual("Bob", results[1].Name);
            Assert.AreEqual("Alice", results[2].Name);
        }
    }

    [TestMethod]
    public void OrderBy_ByNumericField()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_numeric.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Charlie", Age = 30, Email = "c@test.com" });
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "a@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 35, Email = "b@test.com" });

            List<Person> results = db.Query<Person>()
                .OrderBy(PersonMeta.Age)
                .ToList();

            Assert.HasCount(3, results);
            Assert.AreEqual(25, results[0].Age);
            Assert.AreEqual(30, results[1].Age);
            Assert.AreEqual(35, results[2].Age);
        }
    }

    [TestMethod]
    public void OrderBy_WithFilter()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("orderby_filter.db", 20))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.LessThan, 25)
                .OrderByDescending(PersonMeta.Name)
                .ToList();

            for (int i = 0; i < results.Count; ++i)
            {
                Assert.IsLessThan(25, results[i].Age);
            }
            
            for (int i = 1; i < results.Count; ++i)
            {
                Assert.IsGreaterThanOrEqualTo(0, string.Compare(results[i - 1].Name, 
                    results[i].Name, StringComparison.Ordinal));
            }
        }
    }

    [TestMethod]
    public void OrderBy_WithSkipAndLimit()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_paging.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                db.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"p{i}@test.com" });
            }

            List<Person> page1 = db.Query<Person>()
                .OrderBy(PersonMeta.Age)
                .Skip(0)
                .Limit(3)
                .ToList();

            List<Person> page2 = db.Query<Person>()
                .OrderBy(PersonMeta.Age)
                .Skip(3)
                .Limit(3)
                .ToList();

            Assert.HasCount(3, page1);
            Assert.HasCount(3, page2);

            Assert.AreEqual(20, page1[0].Age);
            Assert.AreEqual(21, page1[1].Age);
            Assert.AreEqual(22, page1[2].Age);

            Assert.AreEqual(23, page2[0].Age);
            Assert.AreEqual(24, page2[1].Age);
            Assert.AreEqual(25, page2[2].Age);
        }
    }

    [TestMethod]
    public void OrderBy_MultipleFields()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_multi.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 30, Email = "a1@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "b@test.com" });
            db.Insert(new Person { Name = "Alice", Age = 25, Email = "a2@test.com" });
            db.Insert(new Person { Name = "Bob", Age = 30, Email = "b2@test.com" });

            List<Person> results = db.Query<Person>()
                .OrderBy(PersonMeta.Name)
                .OrderBy(PersonMeta.Age)
                .ToList();

            Assert.HasCount(4, results);
            Assert.AreEqual("Alice", results[0].Name);
            Assert.AreEqual(25, results[0].Age);
            Assert.AreEqual("Alice", results[1].Name);
            Assert.AreEqual(30, results[1].Age);
            Assert.AreEqual("Bob", results[2].Name);
            Assert.AreEqual(25, results[2].Age);
            Assert.AreEqual("Bob", results[3].Name);
            Assert.AreEqual(30, results[3].Age);
        }
    }

    [TestMethod]
    public void OrderBy_EmptyCollection_ReturnsEmpty()
    {
        string dbPath = Path.Combine(_testDirectory, "orderby_empty.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            List<Person> results = db.Query<Person>()
                .OrderBy(PersonMeta.Name)
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void OrderBy_WithIndexedFilter()
    {
        using (GaldrDbInstance db = CreateDatabaseWithPersons("orderby_indexed.db", 50))
        {
            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.StartsWith, "Person00")
                .OrderByDescending(PersonMeta.Age)
                .ToList();

            Assert.HasCount(10, results);
            for (int i = 1; i < results.Count; i++)
            {
                Assert.IsGreaterThanOrEqualTo(results[i].Age, results[i - 1].Age);
            }
        }
    }

    #endregion
}
