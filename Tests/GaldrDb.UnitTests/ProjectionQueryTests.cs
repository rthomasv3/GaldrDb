using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class ProjectionQueryTests
{
    private string _testDbPath;
    private Person[] _testPeople;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"projection_test_{Guid.NewGuid()}.galdr");

        _testPeople = new[]
        {
            new Person { Name = "Alice", Age = 30, Email = "alice@example.com" },
            new Person { Name = "Bob", Age = 25, Email = "bob@example.com" },
            new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" },
            new Person { Name = "Diana", Age = 28, Email = "diana@example.com" },
            new Person { Name = "Eve", Age = 32, Email = "eve@example.com" }
        };
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
        string walPath = _testDbPath + ".wal";
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }
    }

    [TestMethod]
    public void Query_ProjectionType_ReturnsProjectedDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>().ToList();

            Assert.HasCount(5, results);
            foreach (PersonSummary summary in results)
            {
                Assert.IsGreaterThan(0, summary.Id);
                Assert.IsFalse(string.IsNullOrEmpty(summary.Name));
            }
        }
    }

    [TestMethod]
    public void Query_ProjectionWithSourceFieldFilter_FiltersCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 30)
                .ToList();

            Assert.HasCount(2, results);
            Assert.IsTrue(results.Exists(p => p.Name == "Charlie"));
            Assert.IsTrue(results.Exists(p => p.Name == "Eve"));
        }
    }

    [TestMethod]
    public void Query_ProjectionWithProjectionFieldFilter_FiltersCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonSummaryMeta.Name, FieldOp.Equals, "Bob")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Bob", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithSourceFieldStartsWith_FiltersCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Email, FieldOp.StartsWith, "a")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithSkipAndLimit_ReturnsCorrectSubset()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .OrderBy(PersonSummaryMeta.Name)
                .Skip(1)
                .Limit(2)
                .ToList();

            Assert.HasCount(2, results);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithOrderBy_SortsCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .OrderBy(PersonSummaryMeta.Name)
                .ToList();

            Assert.HasCount(5, results);
            Assert.AreEqual("Alice", results[0].Name);
            Assert.AreEqual("Bob", results[1].Name);
            Assert.AreEqual("Charlie", results[2].Name);
            Assert.AreEqual("Diana", results[3].Name);
            Assert.AreEqual("Eve", results[4].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithOrderByDescending_SortsCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .OrderByDescending(PersonSummaryMeta.Name)
                .ToList();

            Assert.HasCount(5, results);
            Assert.AreEqual("Eve", results[0].Name);
            Assert.AreEqual("Diana", results[1].Name);
            Assert.AreEqual("Charlie", results[2].Name);
            Assert.AreEqual("Bob", results[3].Name);
            Assert.AreEqual("Alice", results[4].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionInTransaction_SeesInsertedDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "TestPerson", Age = 40, Email = "test@example.com" });

            List<PersonSummary> results = db.Query<PersonSummary>().ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("TestPerson", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionInTransaction_SeesUpdatedDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            int personId = db.Insert(new Person { Name = "Original", Age = 25, Email = "orig@example.com" });

            using (Transaction tx = db.BeginTransaction())
            {
                Person person = tx.GetById<Person>(personId);
                person.Name = "Updated";
                tx.Replace(person);

                List<PersonSummary> results = tx.Query<PersonSummary>()
                    .Where(PersonSummaryMeta.Id, FieldOp.Equals, personId)
                    .ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("Updated", results[0].Name);
            }
        }
    }

    [TestMethod]
    public void Query_ProjectionInTransaction_DoesNotSeeDeletedDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            int personId;
            using (Transaction tx = db.BeginTransaction())
            {
                personId = tx.Insert(new Person { Name = "ToDelete", Age = 25, Email = "del@example.com" });
                tx.Commit();
            }

            using (Transaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(personId);

                List<PersonSummary> results = tx.Query<PersonSummary>().ToList();

                Assert.IsEmpty(results);
            }
        }
    }

    [TestMethod]
    public void Query_ProjectionFirstOrDefault_ReturnsFirstMatch()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            PersonSummary result = db.Query<PersonSummary>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Charlie")
                .FirstOrDefault();

            Assert.IsNotNull(result);
            Assert.AreEqual("Charlie", result.Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionFirstOrDefault_ReturnsNullWhenNoMatch()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Commit();
            }

            PersonSummary result = db.Query<PersonSummary>()
                .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent")
                .FirstOrDefault();

            Assert.IsNull(result);
        }
    }

    [TestMethod]
    public void Query_ProjectionCount_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            int count = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .Count();

            Assert.AreEqual(3, count);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithMultipleFilters_CombinesFiltersCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 25)
                .Where(PersonMeta.Age, FieldOp.LessThan, 35)
                .ToList();

            Assert.HasCount(3, results);
            Assert.IsTrue(results.Exists(p => p.Name == "Alice"));
            Assert.IsTrue(results.Exists(p => p.Name == "Diana"));
            Assert.IsTrue(results.Exists(p => p.Name == "Eve"));
        }
    }

    [TestMethod]
    public void Query_ProjectionWithMixedSourceAndProjectionFilters_CombinesCorrectly()
    {
        // Test combining source field filters (PersonMeta) and projection field filters (PersonSummaryMeta)
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            // Filter by source field (Age > 25) AND projection field (Name starts with 'A' or 'E')
            // Age > 25: Alice(30), Charlie(35), Diana(28), Eve(32)
            // Names starting with A or E: Alice, Eve
            // Combined: Alice, Eve
            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThan, 25)
                .Where(PersonSummaryMeta.Name, FieldOp.StartsWith, "A")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void Query_ProjectionWithMixedFilters_SourceFilterOnNonProjectedField()
    {
        // Test filtering on a source field that is NOT on the projection (Email is on Person but not PersonSummary)
        // combined with a projection field filter
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            // Filter by Email (source-only field) AND Name (projection field)
            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Email, FieldOp.Contains, "example.com")
                .Where(PersonSummaryMeta.Name, FieldOp.Equals, "Charlie")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Charlie", results[0].Name);
        }
    }

    [TestMethod]
    public void Count_ProjectionUnfiltered_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            int count = db.Query<PersonSummary>().Count();

            Assert.AreEqual(5, count);
        }
    }

    [TestMethod]
    public void Count_ProjectionWithSourceFilterOnly_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            int count = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .Count();

            Assert.AreEqual(3, count);
        }
    }

    [TestMethod]
    public void Count_ProjectionWithProjectionFilterOnly_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            int count = db.Query<PersonSummary>()
                .Where(PersonSummaryMeta.Name, FieldOp.StartsWith, "A")
                .Count();

            Assert.AreEqual(1, count);
        }
    }

    [TestMethod]
    public void Count_ProjectionWithMixedFilters_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            int count = db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 25)
                .Where(PersonSummaryMeta.Name, FieldOp.Contains, "a")
                .Count();

            Assert.AreEqual(2, count);
        }
    }

    [TestMethod]
    public void Count_ProjectionWithWriteSetInsert_IncludesUncommittedInserts()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
                tx.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });

                int count = tx.Query<PersonSummary>().Count();

                Assert.AreEqual(2, count);
            }
        }
    }

    [TestMethod]
    public void Count_ProjectionWithWriteSetDelete_ExcludesDeletedDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    tx.Insert(person);
                }
                tx.Commit();
            }

            using (Transaction tx = db.BeginTransaction())
            {
                tx.DeleteById<Person>(1);

                int count = tx.Query<PersonSummary>().Count();

                Assert.AreEqual(4, count);
            }
        }
    }

    [TestMethod]
    public async Task CountAsync_ProjectionWithSourceFilter_ReturnsCorrectCount()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                foreach (Person person in _testPeople)
                {
                    await tx.InsertAsync(person);
                }
                await tx.CommitAsync();
            }

            int count = await db.Query<PersonSummary>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .CountAsync();

            Assert.AreEqual(3, count);
        }
    }
}
