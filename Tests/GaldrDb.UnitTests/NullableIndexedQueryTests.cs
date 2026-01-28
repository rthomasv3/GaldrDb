using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class NullableIndexedQueryTests
{
    private string _testDbPath;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"nullable_indexed_test_{Guid.NewGuid()}.db");
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
        string walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }
    }

    [TestMethod]
    public void Query_NullableIntIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = i, Name = $"Doc{i}" });
            }
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = null, Name = $"NullDoc{i}" });
            }

            QueryExplanation explanation = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt, FieldOp.Equals, 25)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("NullableInt", explanation.IndexedField);

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt, FieldOp.Equals, 25)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual(25, results[0].NullableInt);
        }
    }

    [TestMethod]
    public void Query_NullableIntIndexedField_RangeQuery_ExcludesNulls()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = i, Name = $"Doc{i}" });
            }
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = null, Name = $"NullDoc{i}" });
            }

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt, FieldOp.GreaterThan, 40)
                .ToList();

            Assert.HasCount(9, results);
            foreach (NullableIndexedDocument doc in results)
            {
                Assert.IsNotNull(doc.NullableInt);
                Assert.IsGreaterThan(40, doc.NullableInt.Value);
            }
        }
    }

    [TestMethod]
    public void Query_NullableInt16IndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (short i = -25; i < 25; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt16 = i, Name = $"Doc{i}" });
            }
            for (int i = 0; i < 25; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt16 = null, Name = $"NullDoc{i}" });
            }

            QueryExplanation explanation = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt16, FieldOp.LessThan, (short)0)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("NullableInt16", explanation.IndexedField);

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt16, FieldOp.LessThan, (short)0)
                .ToList();

            Assert.HasCount(25, results);
            foreach (NullableIndexedDocument doc in results)
            {
                Assert.IsNotNull(doc.NullableInt16);
                Assert.IsLessThan(0, doc.NullableInt16.Value);
            }
        }
    }

    [TestMethod]
    public void Query_NullableSingleIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableSingle = i * 1.5f, Name = $"Doc{i}" });
            }
            for (int i = 0; i < 25; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableSingle = null, Name = $"NullDoc{i}" });
            }

            QueryExplanation explanation = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableSingle, FieldOp.GreaterThanOrEqual, 10.0f)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("NullableSingle", explanation.IndexedField);

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableSingle, FieldOp.GreaterThanOrEqual, 10.0f)
                .Where(NullableIndexedDocumentMeta.NullableSingle, FieldOp.LessThanOrEqual, 30.0f)
                .ToList();

            foreach (NullableIndexedDocument doc in results)
            {
                Assert.IsNotNull(doc.NullableSingle);
                Assert.IsGreaterThanOrEqualTo(10.0f, doc.NullableSingle.Value);
                Assert.IsLessThanOrEqualTo(30.0f, doc.NullableSingle.Value);
            }
        }
    }

    [TestMethod]
    public void Query_NullableTimeSpanIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 24; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableTimeSpan = TimeSpan.FromHours(i), Name = $"Doc{i}" });
            }
            for (int i = 0; i < 12; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableTimeSpan = null, Name = $"NullDoc{i}" });
            }

            QueryExplanation explanation = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableTimeSpan, FieldOp.Equals, TimeSpan.FromHours(12))
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("NullableTimeSpan", explanation.IndexedField);

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableTimeSpan, FieldOp.Equals, TimeSpan.FromHours(12))
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual(TimeSpan.FromHours(12), results[0].NullableTimeSpan);
        }
    }

    [TestMethod]
    public void Query_NullableEnumIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            Priority[] priorities = { Priority.Low, Priority.Medium, Priority.High, Priority.Critical };
            for (int i = 0; i < 40; i++)
            {
                db.Insert(new NullableIndexedDocument { NullablePriority = priorities[i % 4], Name = $"Doc{i}" });
            }
            for (int i = 0; i < 20; i++)
            {
                db.Insert(new NullableIndexedDocument { NullablePriority = null, Name = $"NullDoc{i}" });
            }

            QueryExplanation explanation = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullablePriority, FieldOp.Equals, Priority.Critical)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("NullablePriority", explanation.IndexedField);

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullablePriority, FieldOp.Equals, Priority.Critical)
                .ToList();

            Assert.HasCount(10, results);
            foreach (NullableIndexedDocument doc in results)
            {
                Assert.AreEqual(Priority.Critical, doc.NullablePriority);
            }
        }
    }

    [TestMethod]
    public void Query_NullableEnumIndexedField_RangeQuery_ExcludesNulls()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            Priority[] priorities = { Priority.Low, Priority.Medium, Priority.High, Priority.Critical };
            for (int i = 0; i < 40; i++)
            {
                db.Insert(new NullableIndexedDocument { NullablePriority = priorities[i % 4], Name = $"Doc{i}" });
            }
            for (int i = 0; i < 20; i++)
            {
                db.Insert(new NullableIndexedDocument { NullablePriority = null, Name = $"NullDoc{i}" });
            }

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullablePriority, FieldOp.GreaterThanOrEqual, Priority.High)
                .ToList();

            Assert.HasCount(20, results);
            foreach (NullableIndexedDocument doc in results)
            {
                Assert.IsNotNull(doc.NullablePriority);
                Assert.IsTrue(doc.NullablePriority == Priority.High || doc.NullablePriority == Priority.Critical);
            }
        }
    }

    [TestMethod]
    public void Query_NullableIntIndexedField_EqualsNull_ReturnsNullDocuments()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 50; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = i, Name = $"Doc{i}" });
            }
            for (int i = 0; i < 25; i++)
            {
                db.Insert(new NullableIndexedDocument { NullableInt = null, Name = $"NullDoc{i}" });
            }

            List<NullableIndexedDocument> results = db.Query<NullableIndexedDocument>()
                .Where(NullableIndexedDocumentMeta.NullableInt, FieldOp.Equals, null)
                .ToList();

            Assert.HasCount(25, results);
            foreach (NullableIndexedDocument doc in results)
            {
                Assert.IsNull(doc.NullableInt);
            }
        }
    }
}
