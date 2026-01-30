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
public class NestedIndexTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbNestedIndexTests_{Guid.NewGuid()}");
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

    [TestMethod]
    public void NestedIndex_SingleField_UsesSecondaryIndex()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_single.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Portland", ZipCode = "97201", State = "OR" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Inactive",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98102", State = "WA" }
            });

            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(2, results);
            foreach (PersonWithIndexedAddress person in results)
            {
                Assert.AreEqual("Seattle", person.Address.City);
            }
        }
    }

    [TestMethod]
    public void NestedIndex_Explain_ShowsSecondaryIndexScan()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_explain.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            QueryExplanation explanation = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Address.City", explanation.IndexedField);
        }
    }

    [TestMethod]
    public void NestedIndex_NullParentObject_IndexesAsNull()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_null_parent.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = null
            });

            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(1, seattleResults);
            Assert.AreEqual("Alice", seattleResults[0].Name);

            List<PersonWithIndexedAddress> allResults = db.Query<PersonWithIndexedAddress>()
                .ToList();

            Assert.HasCount(2, allResults);
        }
    }

    [TestMethod]
    public void NestedIndex_UniqueConstraint_ThrowsOnDuplicate()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_unique.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Insert(new PersonWithIndexedAddress
                {
                    Name = "Bob",
                    Status = "Active",
                    Address = new IndexedAddress { City = "Portland", ZipCode = "98101", State = "OR" }
                });
            });

            Assert.Contains("Unique constraint violation", exception.Message);
        }
    }

    [TestMethod]
    public void NestedIndex_CompoundWithNestedPath_UsesCompoundIndex()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_compound.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Portland", ZipCode = "97201", State = "OR" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Inactive",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98102", State = "WA" }
            });

            // First verify single field queries work
            List<PersonWithIndexedAddress> activeResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Status, FieldOp.Equals, "Active")
                .ToList();
            Assert.HasCount(2, activeResults);

            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(2, seattleResults);

            // Now test compound
            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Status, FieldOp.Equals, "Active")
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_CompoundWithNestedPath_Explain_ShowsCompoundIndex()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_compound_explain.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            QueryExplanation explanation = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Status, FieldOp.Equals, "Active")
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Status_Address.City", explanation.IndexedField);
            Assert.AreEqual(2, explanation.FiltersUsedByIndex);
        }
    }

    [TestMethod]
    public void NestedIndex_UpdateNestedProperty_IndexUpdated()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_update.db"))
        {
            PersonWithIndexedAddress person = new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            };
            db.Insert(person);

            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(1, seattleResults);

            person.Address.City = "Portland";
            db.Replace(person);

            seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.IsEmpty(seattleResults);

            List<PersonWithIndexedAddress> portlandResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Portland")
                .ToList();
            Assert.HasCount(1, portlandResults);
        }
    }

    [TestMethod]
    public void NestedIndex_DeleteDocument_IndexEntriesRemoved()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_delete.db"))
        {
            PersonWithIndexedAddress person = new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            };
            db.Insert(person);

            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(1, results);

            db.DeleteById<PersonWithIndexedAddress>(person.Id);

            results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.IsEmpty(results);
        }
    }

    [TestMethod]
    public void NestedIndex_StartsWith_ReturnsMatchingDocuments()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_startswith.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "San Francisco", ZipCode = "94101", State = "CA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Active",
                Address = new IndexedAddress { City = "San Diego", ZipCode = "92101", State = "CA" }
            });

            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.StartsWith, "San")
                .ToList();

            Assert.HasCount(2, results);
            foreach (PersonWithIndexedAddress person in results)
            {
                Assert.StartsWith("San", person.Address.City);
            }
        }
    }

    [TestMethod]
    public void NestedIndex_PersistsAcrossReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "nested_persist.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_NonIndexedNestedField_UsesFullScan()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_non_indexed.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Portland", ZipCode = "97201", State = "OR" }
            });

            // State is not indexed, so this should use full scan
            QueryExplanation explanation = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.State, FieldOp.Equals, "WA")
                .Explain();

            Assert.AreEqual(QueryScanType.FullScan, explanation.ScanType);

            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.State, FieldOp.Equals, "WA")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Alice", results[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_UniqueConstraint_AllowsMultipleNullParents()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_unique_null_parents.db"))
        {
            // Insert multiple documents with null Address (null parent object)
            // These should all be allowed since null values don't count for unique constraints
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = null
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = null
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Active",
                Address = null
            });

            List<PersonWithIndexedAddress> allResults = db.Query<PersonWithIndexedAddress>()
                .ToList();

            Assert.HasCount(3, allResults);
        }
    }

    [TestMethod]
    public void NestedIndex_UniqueConstraint_AllowsMultipleNullPropertyValues()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_unique_null_props.db"))
        {
            // Insert multiple documents with null ZipCode (null nested property value)
            // These should all be allowed since null values don't count for unique constraints
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = null, State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Portland", ZipCode = null, State = "OR" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Active",
                Address = new IndexedAddress { City = "Denver", ZipCode = null, State = "CO" }
            });

            List<PersonWithIndexedAddress> allResults = db.Query<PersonWithIndexedAddress>()
                .ToList();

            Assert.HasCount(3, allResults);
        }
    }

    [TestMethod]
    public void NestedIndex_NullNestedProperty_ExcludedFromNonNullQuery()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_null_property.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = null, ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "97201", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = "Active",
                Address = new IndexedAddress { City = "Portland", ZipCode = "97202", State = "OR" }
            });

            // Query for Seattle should only return Bob
            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(1, seattleResults);
            Assert.AreEqual("Bob", seattleResults[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_CompoundIndex_NullNestedField_ExcludedFromNonNullQuery()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_compound_null_excluded.db"))
        {
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = null, ZipCode = "98101", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Bob",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "97201", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "Charlie",
                Status = null,
                Address = new IndexedAddress { City = "Seattle", ZipCode = "97202", State = "WA" }
            });

            // Query for Active + Seattle should only return Bob
            List<PersonWithIndexedAddress> results = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Status, FieldOp.Equals, "Active")
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Bob", results[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_MixedNullAndNonNull_InsertAndQueryAll()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_mixed_null.db"))
        {
            // Various combinations of null states
            db.Insert(new PersonWithIndexedAddress
            {
                Name = "NullParent",
                Status = "Active",
                Address = null
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "NullCity",
                Status = "Active",
                Address = new IndexedAddress { City = null, ZipCode = "11111", State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "NullZip",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = null, State = "WA" }
            });

            db.Insert(new PersonWithIndexedAddress
            {
                Name = "AllPopulated",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            });

            // All documents should be queryable
            List<PersonWithIndexedAddress> allResults = db.Query<PersonWithIndexedAddress>()
                .ToList();
            Assert.HasCount(4, allResults);

            // Query for Seattle should return NullZip and AllPopulated
            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(2, seattleResults);

            // Active status should return all 4 (null nested fields don't affect Status query)
            List<PersonWithIndexedAddress> activeResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Status, FieldOp.Equals, "Active")
                .ToList();
            Assert.HasCount(4, activeResults);
        }
    }

    [TestMethod]
    public void NestedIndex_UpdateFromNullToValue_IndexUpdated()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_null_to_value.db"))
        {
            PersonWithIndexedAddress person = new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = null
            };
            db.Insert(person);

            // Initially no Seattle results
            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.IsEmpty(seattleResults);

            // Update to have an address
            person.Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" };
            db.Replace(person);

            // Now should find it
            seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(1, seattleResults);
            Assert.AreEqual("Alice", seattleResults[0].Name);
        }
    }

    [TestMethod]
    public void NestedIndex_UpdateFromValueToNull_IndexUpdated()
    {
        using (GaldrDbInstance db = CreateDatabase("nested_value_to_null.db"))
        {
            PersonWithIndexedAddress person = new PersonWithIndexedAddress
            {
                Name = "Alice",
                Status = "Active",
                Address = new IndexedAddress { City = "Seattle", ZipCode = "98101", State = "WA" }
            };
            db.Insert(person);

            // Initially finds Seattle
            List<PersonWithIndexedAddress> seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.HasCount(1, seattleResults);

            // Update to null address
            person.Address = null;
            db.Replace(person);

            // Now should not find Seattle
            seattleResults = db.Query<PersonWithIndexedAddress>()
                .Where(PersonWithIndexedAddressMeta.Address.City, FieldOp.Equals, "Seattle")
                .ToList();
            Assert.IsEmpty(seattleResults);

            // Document should still exist
            List<PersonWithIndexedAddress> allResults = db.Query<PersonWithIndexedAddress>()
                .ToList();
            Assert.HasCount(1, allResults);
            Assert.IsNull(allResults[0].Address);
        }
    }
}
