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
public class IndexedTypesQueryTests
{
    private string _testDbPath;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"indexed_types_test_{Guid.NewGuid()}.db");
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
    }

    [TestMethod]
    public void Query_ByteIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (byte i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { ByteField = i, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.ByteField, FieldOp.Equals, (byte)50)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("ByteField", explanation.IndexedField);
            Assert.AreEqual(1, explanation.FiltersUsedByIndex);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.ByteField, FieldOp.Equals, (byte)50)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual((byte)50, results[0].ByteField);
        }
    }

    [TestMethod]
    public void Query_SByteIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (sbyte i = -50; i < 50; i++)
            {
                db.Insert(new IndexedTypesDocument { SByteField = i, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.SByteField, FieldOp.Equals, (sbyte)-25)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("SByteField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.SByteField, FieldOp.Equals, (sbyte)-25)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual((sbyte)-25, results[0].SByteField);
        }
    }

    [TestMethod]
    public void Query_Int16IndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (short i = -500; i < 500; i += 10)
            {
                db.Insert(new IndexedTypesDocument { Int16Field = i, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.Int16Field, FieldOp.GreaterThan, (short)0)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("Int16Field", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.Int16Field, FieldOp.GreaterThan, (short)0)
                .ToList();

            Assert.HasCount(49, results);
            foreach (IndexedTypesDocument doc in results)
            {
                Assert.IsGreaterThan(0, doc.Int16Field);
            }
        }
    }

    [TestMethod]
    public void Query_UInt16IndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (ushort i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { UInt16Field = (ushort)(i * 100), CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .WhereBetween(IndexedTypesDocumentMeta.UInt16Field, (ushort)1000, (ushort)2000)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("UInt16Field", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .WhereBetween(IndexedTypesDocumentMeta.UInt16Field, (ushort)1000, (ushort)2000)
                .ToList();

            Assert.HasCount(11, results);
        }
    }

    [TestMethod]
    public void Query_UInt32IndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (uint i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { UInt32Field = i * 1000000, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.UInt32Field, FieldOp.LessThan, 10000000U)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("UInt32Field", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.UInt32Field, FieldOp.LessThan, 10000000U)
                .ToList();

            Assert.HasCount(10, results);
        }
    }

    [TestMethod]
    public void Query_UInt64IndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (ulong i = 0; i < 50; i++)
            {
                db.Insert(new IndexedTypesDocument { UInt64Field = i * 10000000000UL, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.UInt64Field, FieldOp.Equals, 100000000000UL)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("UInt64Field", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.UInt64Field, FieldOp.Equals, 100000000000UL)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual(100000000000UL, results[0].UInt64Field);
        }
    }

    [TestMethod]
    public void Query_SingleIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { SingleField = i * 0.5f, CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.SingleField, FieldOp.GreaterThanOrEqual, 25.0f)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("SingleField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.SingleField, FieldOp.GreaterThanOrEqual, 25.0f)
                .ToList();

            Assert.HasCount(50, results);
        }
    }

    [TestMethod]
    public void Query_CharIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (char c = 'A'; c <= 'Z'; c++)
            {
                db.Insert(new IndexedTypesDocument { CharField = c });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.CharField, FieldOp.Equals, 'M')
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("CharField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.CharField, FieldOp.Equals, 'M')
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual('M', results[0].CharField);
        }
    }

    [TestMethod]
    public void Query_TimeSpanIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            for (int i = 0; i < 24; i++)
            {
                db.Insert(new IndexedTypesDocument { TimeSpanField = TimeSpan.FromHours(i), CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.TimeSpanField, FieldOp.GreaterThan, TimeSpan.FromHours(12))
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("TimeSpanField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.TimeSpanField, FieldOp.GreaterThan, TimeSpan.FromHours(12))
                .ToList();

            Assert.HasCount(11, results);
            foreach (IndexedTypesDocument doc in results)
            {
                Assert.IsGreaterThan(TimeSpan.FromHours(12), doc.TimeSpanField);
            }
        }
    }

    [TestMethod]
    public void Query_EnumIndexedField_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            Priority[] priorities = { Priority.Low, Priority.Medium, Priority.High, Priority.Critical };
            for (int i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { PriorityField = priorities[i % 4], CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.PriorityField, FieldOp.Equals, Priority.High)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("PriorityField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.PriorityField, FieldOp.Equals, Priority.High)
                .ToList();

            Assert.HasCount(25, results);
            foreach (IndexedTypesDocument doc in results)
            {
                Assert.AreEqual(Priority.High, doc.PriorityField);
            }
        }
    }

    [TestMethod]
    public void Query_EnumIndexedField_RangeQuery_UsesSecondaryIndex()
    {
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create(_testDbPath, new GaldrDbOptions()))
        {
            Priority[] priorities = { Priority.Low, Priority.Medium, Priority.High, Priority.Critical };
            for (int i = 0; i < 100; i++)
            {
                db.Insert(new IndexedTypesDocument { PriorityField = priorities[i % 4], CharField = 'A' });
            }

            QueryExplanation explanation = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.PriorityField, FieldOp.GreaterThanOrEqual, Priority.High)
                .Explain();

            Assert.AreEqual(QueryScanType.SecondaryIndex, explanation.ScanType);
            Assert.AreEqual("PriorityField", explanation.IndexedField);

            List<IndexedTypesDocument> results = db.Query<IndexedTypesDocument>()
                .Where(IndexedTypesDocumentMeta.PriorityField, FieldOp.GreaterThanOrEqual, Priority.High)
                .ToList();

            Assert.HasCount(50, results);
            foreach (IndexedTypesDocument doc in results)
            {
                Assert.IsTrue(doc.PriorityField == Priority.High || doc.PriorityField == Priority.Critical);
            }
        }
    }
}
