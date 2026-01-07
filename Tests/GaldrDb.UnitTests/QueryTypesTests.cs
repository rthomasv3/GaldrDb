using System;
using GaldrDbCore.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class QueryTypesTests
{
    [TestMethod]
    public void GaldrFieldType_HasAllExpectedValues()
    {
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Int32));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Int64));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.String));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Boolean));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.DateTime));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.DateTimeOffset));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Guid));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Double));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Decimal));
        Assert.IsTrue(Enum.IsDefined(typeof(GaldrFieldType), GaldrFieldType.Complex));
    }

    [TestMethod]
    public void FieldOp_HasAllExpectedValues()
    {
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.Equals));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.NotEquals));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.GreaterThan));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.GreaterThanOrEqual));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.LessThan));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.LessThanOrEqual));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.StartsWith));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.EndsWith));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.Contains));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.Between));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.In));
        Assert.IsTrue(Enum.IsDefined(typeof(FieldOp), FieldOp.NotIn));
    }

    [TestMethod]
    public void GaldrField_ConstructorSetsAllProperties()
    {
        GaldrField<TestDocument, string> field = new GaldrField<TestDocument, string>(
            "Name",
            GaldrFieldType.String,
            true,
            doc => doc.Name);

        Assert.AreEqual("Name", field.FieldName);
        Assert.AreEqual(GaldrFieldType.String, field.FieldType);
        Assert.IsTrue(field.IsIndexed);
        Assert.IsNotNull(field.Accessor);
    }

    [TestMethod]
    public void GaldrField_AccessorReturnsCorrectValue()
    {
        GaldrField<TestDocument, string> nameField = new GaldrField<TestDocument, string>(
            "Name",
            GaldrFieldType.String,
            true,
            doc => doc.Name);

        TestDocument document = new TestDocument { Name = "Alice", Age = 30 };

        string result = nameField.Accessor(document);

        Assert.AreEqual("Alice", result);
    }

    [TestMethod]
    public void GaldrField_AccessorWorksWithIntField()
    {
        GaldrField<TestDocument, int> ageField = new GaldrField<TestDocument, int>(
            "Age",
            GaldrFieldType.Int32,
            false,
            doc => doc.Age);

        TestDocument document = new TestDocument { Name = "Bob", Age = 25 };

        int result = ageField.Accessor(document);

        Assert.AreEqual(25, result);
    }

    [TestMethod]
    public void GaldrField_AccessorWorksWithDateTimeField()
    {
        GaldrField<TestDocument, DateTime> createdField = new GaldrField<TestDocument, DateTime>(
            "Created",
            GaldrFieldType.DateTime,
            true,
            doc => doc.Created);

        DateTime expectedDate = new DateTime(2025, 1, 7, 12, 0, 0);
        TestDocument document = new TestDocument { Name = "Charlie", Age = 35, Created = expectedDate };

        DateTime result = createdField.Accessor(document);

        Assert.AreEqual(expectedDate, result);
    }

    [TestMethod]
    public void GaldrField_AccessorWorksWithGuidField()
    {
        GaldrField<TestDocument, Guid> idField = new GaldrField<TestDocument, Guid>(
            "UniqueId",
            GaldrFieldType.Guid,
            true,
            doc => doc.UniqueId);

        Guid expectedGuid = Guid.NewGuid();
        TestDocument document = new TestDocument { Name = "Diana", Age = 40, UniqueId = expectedGuid };

        Guid result = idField.Accessor(document);

        Assert.AreEqual(expectedGuid, result);
    }

    [TestMethod]
    public void GaldrField_NonIndexedFieldHasCorrectFlag()
    {
        GaldrField<TestDocument, int> field = new GaldrField<TestDocument, int>(
            "Age",
            GaldrFieldType.Int32,
            false,
            doc => doc.Age);

        Assert.IsFalse(field.IsIndexed);
    }

    private class TestDocument
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public DateTime Created { get; set; }
        public Guid UniqueId { get; set; }
    }
}
