using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class ProjectionSourceGeneratorTests
{
    [TestMethod]
    public void PersonSummary_ImplementsIProjectionOf_Person()
    {
        PersonSummary summary = new PersonSummary { Id = 1, Name = "Test" };

        Assert.IsInstanceOfType<IProjectionOf<Person>>(summary);
    }

    [TestMethod]
    public void PersonSummaryMeta_CollectionName_MatchesPersonMeta()
    {
        Assert.AreEqual(PersonMeta.CollectionName, PersonSummaryMeta.CollectionName);
    }

    [TestMethod]
    public void PersonSummaryMeta_SourceType_IsPerson()
    {
        Assert.AreEqual(typeof(Person), PersonSummaryMeta.SourceType);
    }

    [TestMethod]
    public void PersonSummaryMeta_HasIdField()
    {
        GaldrField<PersonSummary, int> idField = PersonSummaryMeta.Id;

        Assert.IsNotNull(idField);
        Assert.AreEqual("Id", idField.FieldName);
        Assert.AreEqual(GaldrFieldType.Int32, idField.FieldType);
    }

    [TestMethod]
    public void PersonSummaryMeta_HasNameField()
    {
        GaldrField<PersonSummary, string> nameField = PersonSummaryMeta.Name;

        Assert.IsNotNull(nameField);
        Assert.AreEqual("Name", nameField.FieldName);
        Assert.AreEqual(GaldrFieldType.String, nameField.FieldType);
    }

    [TestMethod]
    public void PersonSummaryMeta_TypeInfo_IsRegistered()
    {
        IGaldrTypeInfo typeInfo = GaldrTypeRegistry.Get(typeof(PersonSummary));

        Assert.IsNotNull(typeInfo);
        Assert.IsInstanceOfType<IGaldrProjectionTypeInfo>(typeInfo);
    }

    [TestMethod]
    public void PersonSummaryMeta_TypeInfo_HasCorrectCollectionName()
    {
        GaldrProjectionTypeInfo<PersonSummary, Person> typeInfo = PersonSummaryMeta.TypeInfo;

        Assert.AreEqual("Person", typeInfo.CollectionName);
    }

    [TestMethod]
    public void PersonSummaryMeta_TypeInfo_Converter_CreatesValidProjection()
    {
        Person source = new Person
        {
            Id = 42,
            Name = "John Doe",
            Age = 30,
            Email = "john@example.com"
        };

        IGaldrProjectionTypeInfo projTypeInfo = PersonSummaryMeta.TypeInfo;
        object result = projTypeInfo.ConvertToProjection(source);

        Assert.IsInstanceOfType<PersonSummary>(result);
        PersonSummary summary = (PersonSummary)result;
        Assert.AreEqual(42, summary.Id);
        Assert.AreEqual("John Doe", summary.Name);
    }

    [TestMethod]
    public void GaldrTypeRegistry_IsProjection_ReturnsTrueForProjection()
    {
        Assert.IsTrue(GaldrTypeRegistry.IsProjection<PersonSummary>());
    }

    [TestMethod]
    public void GaldrTypeRegistry_IsProjection_ReturnsFalseForCollection()
    {
        Assert.IsFalse(GaldrTypeRegistry.IsProjection<Person>());
    }
}
