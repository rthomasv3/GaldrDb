using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class QueryPlannerTests
{
    private string _testDirectory;
    private string _testDbPath;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "GaldrDbTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");
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
    public void Query_WithIdEquals_ReturnsCorrectDocument()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 100; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.Equals, 50)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Person50", results[0].Name);
            Assert.AreEqual(50, results[0].Id);
        }
    }

    [TestMethod]
    public void Query_WithIdGreaterThan_ReturnsCorrectDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 20; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThan, 15)
                .ToList();

            Assert.HasCount(5, results);

            foreach (Person p in results)
            {
                Assert.IsGreaterThan(15, p.Id);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdGreaterThanOrEqual_ReturnsCorrectDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 20; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThanOrEqual, 15)
                .ToList();

            Assert.HasCount(6, results);

            foreach (Person p in results)
            {
                Assert.IsGreaterThanOrEqualTo(15, p.Id);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdLessThan_ReturnsCorrectDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 20; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.LessThan, 6)
                .ToList();

            Assert.HasCount(5, results);

            foreach (Person p in results)
            {
                Assert.IsLessThan(6, p.Id);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdLessThanOrEqual_ReturnsCorrectDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 20; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.LessThanOrEqual, 5)
                .ToList();

            Assert.HasCount(5, results);

            foreach (Person p in results)
            {
                Assert.IsLessThanOrEqualTo(5, p.Id);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdBetween_ReturnsCorrectDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 50; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .WhereBetween(PersonMeta.Id, 20, 30)
                .ToList();

            Assert.HasCount(11, results);

            foreach (Person p in results)
            {
                Assert.IsTrue(p.Id >= 20 && p.Id <= 30);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdFilterAndOtherFilter_CombinesCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 100; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + (i % 10), Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThan, 50)
                .Where(PersonMeta.Age, FieldOp.Equals, 25)
                .ToList();

            foreach (Person p in results)
            {
                Assert.IsGreaterThan(50, p.Id);
                Assert.AreEqual(25, p.Age);
            }
        }
    }

    [TestMethod]
    public void Query_WithIdEquals_NoMatch_ReturnsEmpty()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 10; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.Equals, 999)
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_WithIdRange_EmptyRange_ReturnsEmpty()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 10; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .WhereBetween(PersonMeta.Id, 100, 200)
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void Query_WithIdFilterInTransaction_SeesUncommittedChanges()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 10; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            using (ITransaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "NewPerson", Age = 99, Email = "new@example.com" });

                List<Person> results = tx.Query<Person>()
                    .Where(PersonMeta.Id, FieldOp.GreaterThan, 10)
                    .ToList();

                Assert.HasCount(1, results);
                Assert.AreEqual("NewPerson", results[0].Name);
            }
        }
    }

    [TestMethod]
    public void Query_Projection_WithIdFilter_ReturnsCorrectProjections()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 50; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .Where(PersonMeta.Id, FieldOp.GreaterThanOrEqual, 40)
                .ToList();

            Assert.HasCount(11, results);

            foreach (PersonSummary p in results)
            {
                Assert.IsGreaterThanOrEqualTo(40, p.Id);
            }
        }
    }

    [TestMethod]
    public void Query_Projection_WithIdBetween_ReturnsCorrectProjections()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 100; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<PersonSummary> results = db.Query<PersonSummary>()
                .WhereBetween(PersonMeta.Id, 25, 35)
                .ToList();

            Assert.HasCount(11, results);

            foreach (PersonSummary p in results)
            {
                Assert.IsTrue(p.Id >= 25 && p.Id <= 35);
            }
        }
    }

    [TestMethod]
    public void Query_WithMultipleIdFilters_UsesFirstIdFilter()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 100; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThan, 50)
                .Where(PersonMeta.Id, FieldOp.LessThan, 60)
                .ToList();

            Assert.HasCount(9, results);

            foreach (Person p in results)
            {
                Assert.IsTrue(p.Id > 50 && p.Id < 60);
            }
        }
    }

    [TestMethod]
    public void Query_LargeDataset_WithIdRange_ReturnsCorrectSubset()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                for (int i = 1; i <= 1000; i++)
                {
                    tx.Insert(new Person { Name = $"Person{i}", Age = 20 + (i % 50), Email = $"person{i}@example.com" });
                }
                tx.Commit();
            }

            List<Person> results = db.Query<Person>()
                .WhereBetween(PersonMeta.Id, 500, 510)
                .ToList();

            Assert.HasCount(11, results);

            foreach (Person p in results)
            {
                Assert.IsTrue(p.Id >= 500 && p.Id <= 510);
            }
        }
    }

    [TestMethod]
    public void Explain_WithNoFilters_ReturnsFullScan()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>().Explain();

            Assert.AreEqual(QueryScanType.FullScan, explanation.ScanType);
            Assert.AreEqual(0, explanation.TotalFilters);
            Assert.AreEqual(0, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
        }
    }

    [TestMethod]
    public void Explain_WithIdEquals_ReturnsPrimaryKeyRange()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.Equals, 1)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("Id", explanation.IndexedField);
            Assert.AreEqual("1", explanation.RangeStart);
            Assert.AreEqual("1", explanation.RangeEnd);
            Assert.IsTrue(explanation.IncludesStart);
            Assert.IsTrue(explanation.IncludesEnd);
            Assert.AreEqual(1, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
            Assert.Contains("Primary key lookup", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_WithIdGreaterThan_ReturnsPrimaryKeyRange()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThan, 10)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("Id", explanation.IndexedField);
            Assert.AreEqual("10", explanation.RangeStart);
            Assert.AreEqual("MAX", explanation.RangeEnd);
            Assert.IsFalse(explanation.IncludesStart);
            Assert.IsTrue(explanation.IncludesEnd);
            Assert.AreEqual(1, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.Contains("Primary key range scan", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_WithIdLessThanOrEqual_ReturnsPrimaryKeyRange()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.LessThanOrEqual, 100)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("Id", explanation.IndexedField);
            Assert.AreEqual("MIN", explanation.RangeStart);
            Assert.AreEqual("100", explanation.RangeEnd);
            Assert.IsTrue(explanation.IncludesStart);
            Assert.IsTrue(explanation.IncludesEnd);
            Assert.AreEqual(1, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
        }
    }

    [TestMethod]
    public void Explain_WithIdBetween_ReturnsPrimaryKeyRange()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .WhereBetween(PersonMeta.Id, 10, 50)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("Id", explanation.IndexedField);
            Assert.AreEqual("10", explanation.RangeStart);
            Assert.AreEqual("50", explanation.RangeEnd);
            Assert.IsTrue(explanation.IncludesStart);
            Assert.IsTrue(explanation.IncludesEnd);
            Assert.AreEqual(1, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
        }
    }

    [TestMethod]
    public void Explain_WithNonIdFilter_ReturnsFullScan()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });
                tx.Commit();
            }

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.Equals, 25)
                .Explain();

            Assert.AreEqual(QueryScanType.FullScan, explanation.ScanType);
            Assert.AreEqual(1, explanation.TotalFilters);
            Assert.AreEqual(0, explanation.FiltersUsedByIndex);
            Assert.AreEqual(1, explanation.FiltersAppliedAfterScan);
        }
    }

    [TestMethod]
    public void Explain_WithIdAndOtherFilters_ShowsCorrectFilterCounts()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.GreaterThan, 10)
                .Where(PersonMeta.Age, FieldOp.Equals, 25)
                .Where(PersonMeta.Name, FieldOp.Equals, "Test")
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual(3, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.AreEqual(2, explanation.FiltersAppliedAfterScan);
        }
    }

    [TestMethod]
    public void Explain_ToStringReturnsDescription()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .Where(PersonMeta.Id, FieldOp.Equals, 42)
                .Explain();

            string description = explanation.ToString();

            Assert.AreEqual(explanation.ScanDescription, description);
            Assert.Contains("42", description);
        }
    }

    [TestMethod]
    public void Explain_InTransaction_WorksCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<Person>()
                .WhereBetween(PersonMeta.Id, 1, 100)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("1", explanation.RangeStart);
            Assert.AreEqual("100", explanation.RangeEnd);
        }
    }

    [TestMethod]
    public void Explain_Projection_WorksCorrectly()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Test", Age = 25, Email = "test@example.com" });

            QueryExplanation explanation = db.Query<PersonSummary>()
                .Where(PersonMeta.Id, FieldOp.GreaterThanOrEqual, 50)
                .Explain();

            Assert.AreEqual(QueryScanType.PrimaryKeyRange, explanation.ScanType);
            Assert.AreEqual("Id", explanation.IndexedField);
            Assert.AreEqual("50", explanation.RangeStart);
        }
    }
}
