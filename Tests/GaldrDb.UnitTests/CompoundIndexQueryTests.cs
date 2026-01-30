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
public class CompoundIndexQueryTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbCompoundIndexTests_{Guid.NewGuid()}");
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

    private GaldrDbInstance CreateDatabase(string dbName)
    {
        string dbPath = Path.Combine(_testDirectory, dbName);
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };
        return GaldrDbInstance.Create(dbPath, options);
    }

    private void InsertTestOrders(GaldrDbInstance db, int count)
    {
        string[] statuses = { "Pending", "Processing", "Shipped", "Delivered" };
        string[] categories = { "Electronics", "Clothing", "Food", "Books" };
        DateTime baseDate = new DateTime(2024, 1, 1);

        for (int i = 0; i < count; i++)
        {
            Order order = new Order
            {
                Status = statuses[i % statuses.Length],
                CreatedDate = baseDate.AddDays(i),
                Category = categories[i % categories.Length],
                Priority = (i % 5) + 1,
                Amount = 100 + (i * 10),
                CustomerName = $"Customer{i:D3}"
            };
            db.Insert(order);
        }
    }

    [TestMethod]
    public void CompoundIndex_FullKeyEquality_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("full_key_equality.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.Equals, new DateTime(2024, 1, 1))
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Pending", results[0].Status);
            Assert.AreEqual(new DateTime(2024, 1, 1), results[0].CreatedDate);
        }
    }

    [TestMethod]
    public void CompoundIndex_PrefixOnlyQuery_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("prefix_only.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .ToList();

            Assert.HasCount(25, results);
            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_EqualityPlusRange_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("equality_plus_range.db"))
        {
            InsertTestOrders(db, 100);

            DateTime rangeStart = new DateTime(2024, 1, 10);
            DateTime rangeEnd = new DateTime(2024, 1, 20);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .WhereBetween(TestModelsOrderMeta.CreatedDate, rangeStart, rangeEnd)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.IsTrue(order.CreatedDate >= rangeStart && order.CreatedDate <= rangeEnd);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_MultipleEqualityFilters_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("multiple_equality.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .Where(TestModelsOrderMeta.Priority, FieldOp.Equals, 1)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Electronics", order.Category);
                Assert.AreEqual(1, order.Priority);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_ThreeFieldIndex_FullMatch()
    {
        using (GaldrDbInstance db = CreateDatabase("three_field_full.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .Where(TestModelsOrderMeta.Priority, FieldOp.Equals, 1)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.AreEqual("Electronics", order.Category);
                Assert.AreEqual(1, order.Priority);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_ThreeFieldIndex_PartialMatch()
    {
        using (GaldrDbInstance db = CreateDatabase("three_field_partial.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.AreEqual("Electronics", order.Category);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_GreaterThanRange_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("greater_than_range.db"))
        {
            InsertTestOrders(db, 100);

            DateTime threshold = new DateTime(2024, 3, 1);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.GreaterThan, threshold)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.IsTrue(order.CreatedDate > threshold);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_LessThanRange_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("less_than_range.db"))
        {
            InsertTestOrders(db, 100);

            DateTime threshold = new DateTime(2024, 2, 1);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.LessThan, threshold)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.IsTrue(order.CreatedDate < threshold);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_NoMatch_ReturnsEmpty()
    {
        using (GaldrDbInstance db = CreateDatabase("no_match.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "NonExistent")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.Equals, new DateTime(2024, 1, 1))
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void CompoundIndex_WithOrderBy_ReturnsOrderedResults()
    {
        using (GaldrDbInstance db = CreateDatabase("with_order_by.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .OrderBy(TestModelsOrderMeta.CreatedDate)
                .ToList();

            Assert.HasCount(25, results);
            for (int i = 1; i < results.Count; i++)
            {
                Assert.IsTrue(results[i].CreatedDate >= results[i - 1].CreatedDate);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_WithOrderByDescending_ReturnsOrderedResults()
    {
        using (GaldrDbInstance db = CreateDatabase("with_order_by_desc.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .OrderByDescending(TestModelsOrderMeta.CreatedDate)
                .ToList();

            Assert.HasCount(25, results);
            for (int i = 1; i < results.Count; i++)
            {
                Assert.IsTrue(results[i].CreatedDate <= results[i - 1].CreatedDate);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_WithSkipAndTake_ReturnsCorrectSubset()
    {
        using (GaldrDbInstance db = CreateDatabase("skip_take.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .OrderBy(TestModelsOrderMeta.CreatedDate)
                .Skip(5)
                .Limit(10)
                .ToList();

            Assert.HasCount(10, results);
        }
    }

    [TestMethod]
    public void CompoundIndex_AfterUpdate_ReflectsChanges()
    {
        using (GaldrDbInstance db = CreateDatabase("after_update.db"))
        {
            InsertTestOrders(db, 10);

            List<Order> initialResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .ToList();

            int initialCount = initialResults.Count;

            if (initialResults.Count > 0)
            {
                Order orderToUpdate = initialResults[0];
                orderToUpdate.Status = "Shipped";
                db.Replace(orderToUpdate);

                List<Order> afterUpdateResults = db.Query<Order>()
                    .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                    .ToList();

                Assert.HasCount(initialCount - 1, afterUpdateResults);

                List<Order> shippedResults = db.Query<Order>()
                    .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Shipped")
                    .ToList();

                Assert.IsTrue(shippedResults.Exists(o => o.Id == orderToUpdate.Id));
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_AfterDelete_ReflectsChanges()
    {
        using (GaldrDbInstance db = CreateDatabase("after_delete.db"))
        {
            InsertTestOrders(db, 10);

            List<Order> initialResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .ToList();

            int initialCount = initialResults.Count;

            if (initialResults.Count > 0)
            {
                Order orderToDelete = initialResults[0];
                db.DeleteById<Order>(orderToDelete.Id);

                List<Order> afterDeleteResults = db.Query<Order>()
                    .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                    .ToList();

                Assert.HasCount(initialCount - 1, afterDeleteResults);
                Assert.IsFalse(afterDeleteResults.Exists(o => o.Id == orderToDelete.Id));
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_PersistsAcrossReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "persist_test.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            InsertTestOrders(db, 50);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.GreaterThanOrEqual, new DateTime(2024, 1, 1))
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_WithNullValue_HandlesCorrectly()
    {
        using (GaldrDbInstance db = CreateDatabase("null_value.db"))
        {
            Order orderWithNull = new Order
            {
                Status = null,
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 100,
                CustomerName = "Customer"
            };
            db.Insert(orderWithNull);

            Order orderWithValue = new Order
            {
                Status = "Pending",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 200,
                CustomerName = "Customer2"
            };
            db.Insert(orderWithValue);

            List<Order> pendingResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .ToList();

            Assert.HasCount(1, pendingResults);
            Assert.AreEqual("Pending", pendingResults[0].Status);
        }
    }

    [TestMethod]
    public void CompoundIndex_IntegerFields_MaintainsSortOrder()
    {
        using (GaldrDbInstance db = CreateDatabase("integer_sort.db"))
        {
            for (int i = 0; i < 20; i++)
            {
                Order order = new Order
                {
                    Status = "Active",
                    CreatedDate = DateTime.Now,
                    Category = "Test",
                    Priority = (i % 5) + 1,
                    Amount = i * 10,
                    CustomerName = $"Customer{i}"
                };
                db.Insert(order);
            }

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Test")
                .Where(TestModelsOrderMeta.Priority, FieldOp.GreaterThan, 2)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Test", order.Category);
                Assert.IsGreaterThan(2, order.Priority);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_MixedFiltersWithNonIndexedField_Works()
    {
        using (GaldrDbInstance db = CreateDatabase("mixed_filters.db"))
        {
            InsertTestOrders(db, 100);

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Amount, FieldOp.GreaterThan, 500m)
                .ToList();

            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.IsGreaterThan(500m, order.Amount);
            }
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_FullKeyMatch_ShowsCorrectInfo()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_full_key.db"))
        {
            InsertTestOrders(db, 10);

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.Equals, new DateTime(2024, 1, 1))
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_CreatedDate", explanation.IndexedField);
            Assert.AreEqual(2, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
            Assert.Contains("Compound index scan", explanation.ScanDescription);
            Assert.Contains("Status_CreatedDate", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_PrefixOnly_ShowsCorrectInfo()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_prefix.db"))
        {
            InsertTestOrders(db, 10);

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_CreatedDate", explanation.IndexedField);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
            Assert.Contains("Compound index scan", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_WithRemainingFilters_ShowsCorrectCounts()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_remaining.db"))
        {
            InsertTestOrders(db, 10);

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Amount, FieldOp.GreaterThan, 500m)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual(2, explanation.TotalFilters);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.AreEqual(1, explanation.FiltersAppliedAfterScan);
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_ThreeFields_ShowsAllMatchedFields()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_three_fields.db"))
        {
            InsertTestOrders(db, 10);

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .Where(TestModelsOrderMeta.Priority, FieldOp.Equals, 1)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_Category_Priority", explanation.IndexedField);
            Assert.AreEqual(3, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
            Assert.Contains("3 fields", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_EqualityPlusRange_ShowsCorrectInfo()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_equality_range.db"))
        {
            InsertTestOrders(db, 10);

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.CreatedDate, FieldOp.GreaterThan, new DateTime(2024, 1, 5))
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_CreatedDate", explanation.IndexedField);
            Assert.AreEqual(2, explanation.FiltersUsedByIndex);
            Assert.Contains("Status Equals", explanation.ScanDescription);
            Assert.Contains("CreatedDate GreaterThan", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void CompoundIndex_NullFirstField_ExcludedFromNonNullQuery()
    {
        using (GaldrDbInstance db = CreateDatabase("null_first_excluded.db"))
        {
            db.Insert(new Order
            {
                Status = null,
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 100,
                CustomerName = "NullStatus"
            });

            db.Insert(new Order
            {
                Status = "Active",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 200,
                CustomerName = "ActiveStatus"
            });

            db.Insert(new Order
            {
                Status = "Pending",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 300,
                CustomerName = "PendingStatus"
            });

            List<Order> activeResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Active")
                .ToList();

            Assert.HasCount(1, activeResults);
            Assert.AreEqual("Active", activeResults[0].Status);
        }
    }

    [TestMethod]
    public void CompoundIndex_NullSecondField_HandlesCorrectly()
    {
        using (GaldrDbInstance db = CreateDatabase("null_second.db"))
        {
            db.Insert(new Order
            {
                Status = "Pending",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = null,
                Priority = 1,
                Amount = 100,
                CustomerName = "NullCategory"
            });

            db.Insert(new Order
            {
                Status = "Pending",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 200,
                CustomerName = "WithCategory"
            });

            List<Order> electronicsResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .Where(TestModelsOrderMeta.Priority, FieldOp.Equals, 1)
                .ToList();

            Assert.HasCount(1, electronicsResults);
            Assert.AreEqual("Electronics", electronicsResults[0].Category);
        }
    }

    [TestMethod]
    public void CompoundIndex_SortOrderPreserved_AcrossMultipleInserts()
    {
        using (GaldrDbInstance db = CreateDatabase("sort_preserved.db"))
        {
            db.Insert(new Order { Status = "B", CreatedDate = new DateTime(2024, 3, 1), Category = "X", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "A", CreatedDate = new DateTime(2024, 1, 1), Category = "X", Priority = 1, Amount = 100, CustomerName = "C2" });
            db.Insert(new Order { Status = "A", CreatedDate = new DateTime(2024, 2, 1), Category = "X", Priority = 1, Amount = 100, CustomerName = "C3" });
            db.Insert(new Order { Status = "C", CreatedDate = new DateTime(2024, 1, 1), Category = "X", Priority = 1, Amount = 100, CustomerName = "C4" });

            List<Order> statusAResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "A")
                .OrderBy(TestModelsOrderMeta.CreatedDate)
                .ToList();

            Assert.HasCount(2, statusAResults);
            Assert.AreEqual(new DateTime(2024, 1, 1), statusAResults[0].CreatedDate);
            Assert.AreEqual(new DateTime(2024, 2, 1), statusAResults[1].CreatedDate);
        }
    }

    [TestMethod]
    public void CompoundIndex_NegativeIntegers_MaintainSortOrder()
    {
        using (GaldrDbInstance db = CreateDatabase("negative_int_sort.db"))
        {
            db.Insert(new Order { Status = "Test", CreatedDate = DateTime.Now, Category = "A", Priority = -5, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Test", CreatedDate = DateTime.Now, Category = "A", Priority = 0, Amount = 100, CustomerName = "C2" });
            db.Insert(new Order { Status = "Test", CreatedDate = DateTime.Now, Category = "A", Priority = 5, Amount = 100, CustomerName = "C3" });
            db.Insert(new Order { Status = "Test", CreatedDate = DateTime.Now, Category = "A", Priority = -10, Amount = 100, CustomerName = "C4" });

            // First verify the equality query works
            List<Order> categoryAResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "A")
                .ToList();

            Assert.HasCount(4, categoryAResults);

            // Now test the range query - should return priorities -5, 0, 5 (all > -6)
            List<Order> rangeResults = db.Query<Order>()
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "A")
                .Where(TestModelsOrderMeta.Priority, FieldOp.GreaterThan, -6)
                .ToList();

            Assert.HasCount(3, rangeResults);
            foreach (Order order in rangeResults)
            {
                Assert.IsGreaterThan(-6, order.Priority);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_AllFieldsNull_InsertAndQuery()
    {
        using (GaldrDbInstance db = CreateDatabase("all_null.db"))
        {
            db.Insert(new Order
            {
                Status = null,
                CreatedDate = new DateTime(2024, 1, 1),
                Category = null,
                Priority = 0,
                Amount = 100,
                CustomerName = "AllNull"
            });

            db.Insert(new Order
            {
                Status = "Active",
                CreatedDate = new DateTime(2024, 1, 1),
                Category = "Electronics",
                Priority = 1,
                Amount = 200,
                CustomerName = "Normal"
            });

            List<Order> allOrders = db.Query<Order>().ToList();
            Assert.HasCount(2, allOrders);

            List<Order> activeOrders = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Active")
                .ToList();
            Assert.HasCount(1, activeOrders);
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithOnSecondField_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_second_field.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electrical", Priority = 2, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Food", Priority = 3, Amount = 300, CustomerName = "C3" });
            db.Insert(new Order { Status = "Shipped", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 400, CustomerName = "C4" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Clothing", Priority = 2, Amount = 500, CustomerName = "C5" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "Elec")
                .ToList();

            Assert.HasCount(2, results);
            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.StartsWith("Elec", order.Category);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithOnSecondField_NoMatches_ReturnsEmpty()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_no_match.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Food", Priority = 2, Amount = 200, CustomerName = "C2" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "Cloth")
                .ToList();

            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithEmptyPrefix_ReturnsAllMatchingFirstField()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_empty.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Food", Priority = 2, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Shipped", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 300, CustomerName = "C3" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "")
                .ToList();

            Assert.HasCount(2, results);
            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithPrefixMatchesMultiple_ReturnsAll()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_multiple.db"))
        {
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "Books", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "BookStore", Priority = 2, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "Bookmarks", Priority = 3, Amount = 300, CustomerName = "C3" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "Bottles", Priority = 4, Amount = 400, CustomerName = "C4" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "Art", Priority = 5, Amount = 500, CustomerName = "C5" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Active")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "Book")
                .ToList();

            Assert.HasCount(3, results);
            foreach (Order order in results)
            {
                Assert.AreEqual("Active", order.Status);
                Assert.StartsWith("Book", order.Category);
            }
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_StartsWith_ShowsCorrectInfo()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_startswith.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "Elec")
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_Category_Priority", explanation.IndexedField);
            Assert.AreEqual(2, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
            Assert.Contains("Compound index scan", explanation.ScanDescription);
            Assert.Contains("Status Equals", explanation.ScanDescription);
            Assert.Contains("Category StartsWith", explanation.ScanDescription);
        }
    }

    [TestMethod]
    public void Explain_CompoundIndex_StartsWithOnly_ShowsCorrectInfo()
    {
        using (GaldrDbInstance db = CreateDatabase("explain_startswith_only.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });

            QueryExplanation explanation = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.StartsWith, "Pend")
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);
            Assert.AreEqual(0, explanation.FiltersAppliedAfterScan);
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithOnThirdField_UsesIndexForFirstTwo()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_third_field.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 10, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 15, Amount = 300, CustomerName = "C3" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 2, Amount = 400, CustomerName = "C4" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Food", Priority = 1, Amount = 500, CustomerName = "C5" });

            // Note: Priority is int so StartsWith doesn't apply directly
            // This test verifies equality + equality + additional filter pattern
            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.Equals, "Electronics")
                .Where(TestModelsOrderMeta.Priority, FieldOp.GreaterThan, 5)
                .ToList();

            Assert.HasCount(2, results);
            foreach (Order order in results)
            {
                Assert.AreEqual("Pending", order.Status);
                Assert.AreEqual("Electronics", order.Category);
                Assert.IsGreaterThan(5, order.Priority);
            }
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithCaseSensitive_MatchesCorrectCase()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_case.db"))
        {
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "Electronics", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "electronics", Priority = 2, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Pending", CreatedDate = DateTime.Now, Category = "ELECTRONICS", Priority = 3, Amount = 300, CustomerName = "C3" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Pending")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "Elec")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Electronics", results[0].Category);
        }
    }

    [TestMethod]
    public void CompoundIndex_StartsWithSpecialCharacters_Works()
    {
        using (GaldrDbInstance db = CreateDatabase("startswith_special.db"))
        {
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "A-B-C", Priority = 1, Amount = 100, CustomerName = "C1" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "A-B-D", Priority = 2, Amount = 200, CustomerName = "C2" });
            db.Insert(new Order { Status = "Active", CreatedDate = DateTime.Now, Category = "A-C", Priority = 3, Amount = 300, CustomerName = "C3" });

            List<Order> results = db.Query<Order>()
                .Where(TestModelsOrderMeta.Status, FieldOp.Equals, "Active")
                .Where(TestModelsOrderMeta.Category, FieldOp.StartsWith, "A-B")
                .ToList();

            Assert.HasCount(2, results);
            foreach (Order order in results)
            {
                Assert.StartsWith("A-B", order.Category);
            }
        }
    }
}
