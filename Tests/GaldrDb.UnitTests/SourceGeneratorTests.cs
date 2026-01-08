using System;
using System.Collections.Generic;
using System.Linq;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class SourceGeneratorTests
{
    [TestMethod]
    public void PersonMeta_CollectionName_ReturnsCorrectName()
    {
        // Arrange & Act
        string collectionName = PersonMeta.CollectionName;

        // Assert
        Assert.AreEqual("Person", collectionName);
    }

    [TestMethod]
    public void PersonMeta_IndexedFieldNames_ContainsName()
    {
        // Arrange & Act
        IReadOnlyList<string> indexedFields = PersonMeta.IndexedFieldNames;

        // Assert
        Assert.IsTrue(indexedFields.Contains("Name"));
    }

    [TestMethod]
    public void PersonMeta_IdField_HasCorrectType()
    {
        // Arrange & Act
        GaldrField<Person, int> idField = PersonMeta.Id;

        // Assert
        Assert.AreEqual("Id", idField.FieldName);
        Assert.AreEqual(GaldrFieldType.Int32, idField.FieldType);
        Assert.IsFalse(idField.IsIndexed);
    }

    [TestMethod]
    public void PersonMeta_NameField_IsIndexed()
    {
        // Arrange & Act
        GaldrField<Person, string> nameField = PersonMeta.Name;

        // Assert
        Assert.AreEqual("Name", nameField.FieldName);
        Assert.AreEqual(GaldrFieldType.String, nameField.FieldType);
        Assert.IsTrue(nameField.IsIndexed);
    }

    [TestMethod]
    public void PersonMeta_AgeField_NotIndexed()
    {
        // Arrange & Act
        GaldrField<Person, int> ageField = PersonMeta.Age;

        // Assert
        Assert.AreEqual("Age", ageField.FieldName);
        Assert.AreEqual(GaldrFieldType.Int32, ageField.FieldType);
        Assert.IsFalse(ageField.IsIndexed);
    }

    [TestMethod]
    public void PersonMeta_FieldAccessor_ReturnsCorrectValue()
    {
        // Arrange
        Person person = new Person { Id = 42, Name = "John", Age = 30, Email = "john@example.com" };

        // Act
        int id = PersonMeta.Id.Accessor(person);
        string name = PersonMeta.Name.Accessor(person);
        int age = PersonMeta.Age.Accessor(person);

        // Assert
        Assert.AreEqual(42, id);
        Assert.AreEqual("John", name);
        Assert.AreEqual(30, age);
    }

    [TestMethod]
    public void PersonMeta_TypeInfo_HasCorrectProperties()
    {
        // Arrange & Act
        GaldrTypeInfo<Person> typeInfo = PersonMeta.TypeInfo;

        // Assert
        Assert.AreEqual("Person", typeInfo.CollectionName);
        Assert.IsTrue(typeInfo.IndexedFieldNames.Contains("Name"));
    }

    [TestMethod]
    public void PersonMeta_TypeInfo_IdGetterWorks()
    {
        // Arrange
        Person person = new Person { Id = 123 };
        GaldrTypeInfo<Person> typeInfo = PersonMeta.TypeInfo;

        // Act
        int id = typeInfo.IdGetter(person);

        // Assert
        Assert.AreEqual(123, id);
    }

    [TestMethod]
    public void PersonMeta_TypeInfo_IdSetterWorks()
    {
        // Arrange
        Person person = new Person();
        GaldrTypeInfo<Person> typeInfo = PersonMeta.TypeInfo;

        // Act
        typeInfo.IdSetter(person, 456);

        // Assert
        Assert.AreEqual(456, person.Id);
    }

    [TestMethod]
    public void PersonMeta_TypeInfo_ExtractIndexedFieldsWorks()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Alice", Age = 25, Email = "alice@test.com" };
        GaldrTypeInfo<Person> typeInfo = PersonMeta.TypeInfo;
        IndexFieldWriter writer = new IndexFieldWriter();

        // Act
        typeInfo.ExtractIndexedFields(person, writer);
        IReadOnlyList<IndexFieldEntry> fields = writer.GetFields();

        // Assert
        Assert.HasCount(1, fields);
        Assert.AreEqual("Name", fields[0].FieldName);
    }

    [TestMethod]
    public void GaldrTypeRegistry_Get_ReturnsPerson()
    {
        // Arrange & Act
        GaldrTypeInfo<Person> typeInfo = GaldrTypeRegistry.Get<Person>();

        // Assert
        Assert.IsNotNull(typeInfo);
        Assert.AreEqual("Person", typeInfo.CollectionName);
    }

    [TestMethod]
    public void GaldrTypeRegistry_GetByType_ReturnsPerson()
    {
        // Arrange & Act
        IGaldrTypeInfo typeInfo = GaldrTypeRegistry.Get(typeof(Person));

        // Assert
        Assert.IsNotNull(typeInfo);
        Assert.AreEqual("Person", typeInfo.CollectionName);
    }

    [TestMethod]
    public void GaldrTypeRegistry_TryGet_ReturnsTrueForPerson()
    {
        // Act
        bool found = GaldrTypeRegistry.TryGet<Person>(out GaldrTypeInfo<Person> typeInfo);

        // Assert
        Assert.IsTrue(found);
        Assert.IsNotNull(typeInfo);
        Assert.AreEqual("Person", typeInfo.CollectionName);
    }

    [TestMethod]
    public void GaldrTypeRegistry_GetAll_ContainsPerson()
    {
        // Act
        IEnumerable<IGaldrTypeInfo> allTypes = GaldrTypeRegistry.GetAll();

        // Assert
        bool containsPerson = false;
        foreach (IGaldrTypeInfo info in allTypes)
        {
            if (info.CollectionName == "Person")
            {
                containsPerson = true;
                break;
            }
        }
        Assert.IsTrue(containsPerson);
    }
}
