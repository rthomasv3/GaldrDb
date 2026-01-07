using System;
using System.Collections.Generic;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class TypeInfoTests
{
    [TestMethod]
    public void IndexFieldWriter_WriteString_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteString("Name", "Alice");

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Name", fields[0].FieldName);
        Assert.IsNotNull(fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteString_NullValue_DoesNotAddField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteString("Name", null);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.IsEmpty(fields);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteInt32_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteInt32("Age", 25);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Age", fields[0].FieldName);
        Assert.HasCount(4, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteInt64_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteInt64("BigNumber", 9876543210L);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("BigNumber", fields[0].FieldName);
        Assert.HasCount(8, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteDouble_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteDouble("Price", 19.99);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Price", fields[0].FieldName);
        Assert.HasCount(8, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteDecimal_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteDecimal("Amount", 123.45m);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Amount", fields[0].FieldName);
        Assert.HasCount(16, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteBoolean_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteBoolean("IsActive", true);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("IsActive", fields[0].FieldName);
        Assert.HasCount(1, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteDateTime_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteDateTime("Created", new DateTime(2025, 1, 7));

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Created", fields[0].FieldName);
        Assert.HasCount(8, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteDateTimeOffset_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteDateTimeOffset("Timestamp", new DateTimeOffset(2025, 1, 7, 12, 0, 0, TimeSpan.Zero));

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("Timestamp", fields[0].FieldName);
        Assert.HasCount(16, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_WriteGuid_AddsField()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteGuid("UniqueId", Guid.NewGuid());

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(1, fields);
        Assert.AreEqual("UniqueId", fields[0].FieldName);
        Assert.HasCount(16, fields[0].KeyBytes);
    }

    [TestMethod]
    public void IndexFieldWriter_MultipleFields_AddsAllFields()
    {
        IndexFieldWriter writer = new IndexFieldWriter();

        writer.WriteString("Name", "Bob");
        writer.WriteInt32("Age", 30);
        writer.WriteBoolean("Active", true);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(3, fields);
        Assert.AreEqual("Name", fields[0].FieldName);
        Assert.AreEqual("Age", fields[1].FieldName);
        Assert.AreEqual("Active", fields[2].FieldName);
    }

    [TestMethod]
    public void IndexFieldWriter_Clear_RemovesAllFields()
    {
        IndexFieldWriter writer = new IndexFieldWriter();
        writer.WriteString("Name", "Alice");
        writer.WriteInt32("Age", 25);

        writer.Clear();

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.IsEmpty(fields);
    }

    [TestMethod]
    public void GaldrTypeInfo_ConstructorSetsAllProperties()
    {
        List<string> indexedFields = new List<string> { "Name", "Email" };
        Action<TestDocument, int> idSetter = (doc, id) => doc.Id = id;
        Func<TestDocument, int> idGetter = doc => doc.Id;
        Action<TestDocument, IndexFieldWriter> extractFields = (doc, writer) =>
        {
            writer.WriteString("Name", doc.Name);
        };

        List<string> uniqueIndexFields = new List<string> { "Email" };

        GaldrTypeInfo<TestDocument> typeInfo = new GaldrTypeInfo<TestDocument>(
            "TestDocuments",
            indexedFields,
            uniqueIndexFields,
            idSetter,
            idGetter,
            extractFields);

        Assert.AreEqual("TestDocuments", typeInfo.CollectionName);
        Assert.HasCount(2, typeInfo.IndexedFieldNames);
        Assert.AreEqual("Name", typeInfo.IndexedFieldNames[0]);
        Assert.AreEqual("Email", typeInfo.IndexedFieldNames[1]);
        Assert.HasCount(1, typeInfo.UniqueIndexFieldNames);
        Assert.AreEqual("Email", typeInfo.UniqueIndexFieldNames[0]);
        Assert.IsNotNull(typeInfo.IdSetter);
        Assert.IsNotNull(typeInfo.IdGetter);
        Assert.IsNotNull(typeInfo.ExtractIndexedFields);
    }

    [TestMethod]
    public void GaldrTypeInfo_IdSetter_SetsIdOnDocument()
    {
        GaldrTypeInfo<TestDocument> typeInfo = CreateTestTypeInfo();
        TestDocument doc = new TestDocument { Name = "Test" };

        typeInfo.IdSetter(doc, 42);

        Assert.AreEqual(42, doc.Id);
    }

    [TestMethod]
    public void GaldrTypeInfo_IdGetter_GetsIdFromDocument()
    {
        GaldrTypeInfo<TestDocument> typeInfo = CreateTestTypeInfo();
        TestDocument doc = new TestDocument { Id = 123, Name = "Test" };

        int id = typeInfo.IdGetter(doc);

        Assert.AreEqual(123, id);
    }

    [TestMethod]
    public void GaldrTypeInfo_ExtractIndexedFields_ExtractsFields()
    {
        GaldrTypeInfo<TestDocument> typeInfo = CreateTestTypeInfo();
        TestDocument doc = new TestDocument { Id = 1, Name = "Alice", Email = "alice@example.com" };
        IndexFieldWriter writer = new IndexFieldWriter();

        typeInfo.ExtractIndexedFields(doc, writer);

        IReadOnlyList<(string FieldName, byte[] KeyBytes)> fields = writer.GetFields();
        Assert.HasCount(2, fields);
        Assert.AreEqual("Name", fields[0].FieldName);
        Assert.AreEqual("Email", fields[1].FieldName);
    }

    [TestMethod]
    public void GaldrTypeInfo_ImplementsIGaldrTypeInfo()
    {
        GaldrTypeInfo<TestDocument> typeInfo = CreateTestTypeInfo();

        IGaldrTypeInfo interfaceRef = typeInfo;

        Assert.AreEqual("TestDocuments", interfaceRef.CollectionName);
        Assert.HasCount(2, interfaceRef.IndexedFieldNames);
    }

    [TestMethod]
    public void GaldrTypeInfo_EmptyIndexedFields_WorksCorrectly()
    {
        List<string> indexedFields = new List<string>();
        List<string> uniqueIndexFields = new List<string>();
        GaldrTypeInfo<TestDocument> typeInfo = new GaldrTypeInfo<TestDocument>(
            "TestDocuments",
            indexedFields,
            uniqueIndexFields,
            (doc, id) => doc.Id = id,
            doc => doc.Id,
            (doc, writer) => { });

        Assert.IsEmpty(typeInfo.IndexedFieldNames);
        Assert.IsEmpty(typeInfo.UniqueIndexFieldNames);
    }

    private GaldrTypeInfo<TestDocument> CreateTestTypeInfo()
    {
        List<string> indexedFields = new List<string> { "Name", "Email" };
        List<string> uniqueIndexFields = new List<string> { "Email" };

        GaldrTypeInfo<TestDocument> result = new GaldrTypeInfo<TestDocument>(
            "TestDocuments",
            indexedFields,
            uniqueIndexFields,
            (doc, id) => doc.Id = id,
            doc => doc.Id,
            (doc, writer) =>
            {
                writer.WriteString("Name", doc.Name);
                writer.WriteString("Email", doc.Email);
            });

        return result;
    }

    private class TestDocument
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
    }
}
