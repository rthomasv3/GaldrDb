using System;
using System.Collections.Generic;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class FilterTests
{
    #region FieldFilter - Equals Tests

    [TestMethod]
    public void FieldFilter_Equals_String_MatchesExactValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Equals, "John");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_Equals_String_DoesNotMatchDifferentValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Equals, "Jane");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void FieldFilter_Equals_Int_MatchesExactValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.Equals, 30);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    #endregion

    #region FieldFilter - NotEquals Tests

    [TestMethod]
    public void FieldFilter_NotEquals_ReturnsTrueForDifferentValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.NotEquals, "Jane");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_NotEquals_ReturnsFalseForSameValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.NotEquals, "John");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    #endregion

    #region FieldFilter - Comparison Tests

    [TestMethod]
    public void FieldFilter_GreaterThan_ReturnsTrueWhenFieldIsGreater()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.GreaterThan, 25);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_GreaterThan_ReturnsFalseWhenFieldIsEqual()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.GreaterThan, 30);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void FieldFilter_GreaterThanOrEqual_ReturnsTrueWhenFieldIsEqual()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_LessThan_ReturnsTrueWhenFieldIsLess()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.LessThan, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_LessThanOrEqual_ReturnsTrueWhenFieldIsEqual()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        FieldFilter<Person, int> filter = new FieldFilter<Person, int>(
            PersonMeta.Age, FieldOp.LessThanOrEqual, 30);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    #endregion

    #region FieldFilter - String Operations Tests

    [TestMethod]
    public void FieldFilter_StartsWith_MatchesPrefix()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Jonathan", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.StartsWith, "Jon");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_StartsWith_DoesNotMatchNonPrefix()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Jonathan", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.StartsWith, "than");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void FieldFilter_EndsWith_MatchesSuffix()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Jonathan", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.EndsWith, "than");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_Contains_MatchesSubstring()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Jonathan", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Contains, "nat");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void FieldFilter_Contains_DoesNotMatchAbsentSubstring()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Jonathan", Age = 30 };
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Contains, "xyz");

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    #endregion

    #region FieldFilter - Properties Tests

    [TestMethod]
    public void FieldFilter_Properties_AreCorrect()
    {
        // Arrange
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Equals, "John");

        // Assert
        Assert.AreEqual("Name", filter.FieldName);
        Assert.AreEqual(GaldrFieldType.String, filter.FieldType);
        Assert.IsTrue(filter.IsIndexed);
        Assert.AreEqual(FieldOp.Equals, filter.Operation);
    }

    #endregion

    #region BetweenFilter Tests

    [TestMethod]
    public void BetweenFilter_InclusiveRange_MatchesValueInRange()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void BetweenFilter_InclusiveRange_MatchesMinValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 25 };
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void BetweenFilter_InclusiveRange_MatchesMaxValue()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 35 };
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void BetweenFilter_DoesNotMatchBelowRange()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 20 };
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void BetweenFilter_DoesNotMatchAboveRange()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 40 };
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void BetweenFilter_Properties_AreCorrect()
    {
        // Arrange
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Assert
        Assert.AreEqual("Age", filter.FieldName);
        Assert.AreEqual(GaldrFieldType.Int32, filter.FieldType);
        Assert.IsFalse(filter.IsIndexed);
        Assert.AreEqual(FieldOp.Between, filter.Operation);
        Assert.AreEqual(25, filter.MinValue);
        Assert.AreEqual(35, filter.MaxValue);
    }

    #endregion

    #region InFilter Tests

    [TestMethod]
    public void InFilter_MatchesValueInSet()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        InFilter<Person, string> filter = new InFilter<Person, string>(
            PersonMeta.Name, new[] { "John", "Jane", "Bob" });

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void InFilter_DoesNotMatchValueNotInSet()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "Alice", Age = 30 };
        InFilter<Person, string> filter = new InFilter<Person, string>(
            PersonMeta.Name, new[] { "John", "Jane", "Bob" });

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void InFilter_WorksWithIntValues()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        InFilter<Person, int> filter = new InFilter<Person, int>(
            PersonMeta.Age, new[] { 25, 30, 35 });

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void InFilter_EmptySet_NeverMatches()
    {
        // Arrange
        Person person = new Person { Id = 1, Name = "John", Age = 30 };
        InFilter<Person, string> filter = new InFilter<Person, string>(
            PersonMeta.Name, Array.Empty<string>());

        // Act
        bool result = filter.Evaluate(person);

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void InFilter_Properties_AreCorrect()
    {
        // Arrange
        InFilter<Person, string> filter = new InFilter<Person, string>(
            PersonMeta.Name, new[] { "John", "Jane" });

        // Assert
        Assert.AreEqual("Name", filter.FieldName);
        Assert.AreEqual(GaldrFieldType.String, filter.FieldType);
        Assert.IsTrue(filter.IsIndexed);
        Assert.AreEqual(FieldOp.In, filter.Operation);
        Assert.HasCount(2, filter.Values);
    }

    #endregion

    #region Index Key Encoding Tests

    [TestMethod]
    public void FieldFilter_GetIndexKeyBytes_EncodesStringValue()
    {
        // Arrange
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Equals, "John");

        // Act
        byte[] keyBytes = filter.GetIndexKeyBytes();

        // Assert
        Assert.IsNotNull(keyBytes);
        Assert.IsNotEmpty(keyBytes);
    }

    [TestMethod]
    public void FieldFilter_GetIndexKeyEndBytes_ReturnsNullForNonStartsWith()
    {
        // Arrange
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.Equals, "John");

        // Act
        byte[] endBytes = filter.GetIndexKeyEndBytes();

        // Assert
        Assert.IsNull(endBytes);
    }

    [TestMethod]
    public void FieldFilter_GetIndexKeyEndBytes_ReturnsPrefixEndForStartsWith()
    {
        // Arrange
        FieldFilter<Person, string> filter = new FieldFilter<Person, string>(
            PersonMeta.Name, FieldOp.StartsWith, "Jo");

        // Act
        byte[] endBytes = filter.GetIndexKeyEndBytes();

        // Assert
        Assert.IsNotNull(endBytes);
    }

    [TestMethod]
    public void BetweenFilter_GetIndexKeyBytes_EncodesMinValue()
    {
        // Arrange
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        byte[] keyBytes = filter.GetIndexKeyBytes();
        byte[] expectedMinBytes = IndexKeyEncoder.Encode(25, GaldrFieldType.Int32);

        // Assert
        CollectionAssert.AreEqual(expectedMinBytes, keyBytes);
    }

    [TestMethod]
    public void BetweenFilter_GetIndexKeyEndBytes_EncodesMaxValue()
    {
        // Arrange
        BetweenFilter<Person, int> filter = new BetweenFilter<Person, int>(
            PersonMeta.Age, 25, 35);

        // Act
        byte[] endBytes = filter.GetIndexKeyEndBytes();
        byte[] expectedMaxBytes = IndexKeyEncoder.Encode(35, GaldrFieldType.Int32);

        // Assert
        CollectionAssert.AreEqual(expectedMaxBytes, endBytes);
    }

    [TestMethod]
    public void InFilter_GetAllIndexKeyBytes_EncodesAllValues()
    {
        // Arrange
        InFilter<Person, int> filter = new InFilter<Person, int>(
            PersonMeta.Age, new[] { 25, 30, 35 });

        // Act
        List<byte[]> allKeys = new List<byte[]>(filter.GetAllIndexKeyBytes());

        // Assert
        Assert.HasCount(3, allKeys);
    }

    #endregion
}
