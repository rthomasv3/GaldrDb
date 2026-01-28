using System.Collections.Generic;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Query.Execution;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class QueryBuilderTests
{
    private List<Person> _testData;
    private InMemoryQueryExecutor<Person> _executor;

    [TestInitialize]
    public void Setup()
    {
        _testData = new List<Person>
        {
            new Person { Id = 1, Name = "Alice", Age = 25, Email = "alice@example.com" },
            new Person { Id = 2, Name = "Bob", Age = 30, Email = "bob@example.com" },
            new Person { Id = 3, Name = "Charlie", Age = 35, Email = "charlie@test.com" },
            new Person { Id = 4, Name = "Diana", Age = 28, Email = "diana@example.com" },
            new Person { Id = 5, Name = "Eve", Age = 32, Email = "eve@test.com" },
            new Person { Id = 6, Name = "Frank", Age = 45, Email = "frank@example.com" },
            new Person { Id = 7, Name = "Grace", Age = 22, Email = "grace@test.com" },
            new Person { Id = 8, Name = "Henry", Age = 38, Email = "henry@example.com" },
            new Person { Id = 9, Name = "Ivy", Age = 29, Email = "ivy@test.com" },
            new Person { Id = 10, Name = "Jack", Age = 33, Email = "jack@example.com" }
        };
        _executor = new InMemoryQueryExecutor<Person>(_testData);
    }

    #region Basic Where Tests

    [TestMethod]
    public void QueryBuilder_Where_Equals_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.Equals, "Bob");

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(1, results);
        Assert.AreEqual("Bob", results[0].Name);
    }

    [TestMethod]
    public void QueryBuilder_Where_GreaterThan_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 35);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(2, results); // Frank (45) and Henry (38)
    }

    [TestMethod]
    public void QueryBuilder_Where_StartsWith_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Email, FieldOp.EndsWith, "@test.com");

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(4, results); // Charlie, Eve, Grace, Ivy
    }

    #endregion

    #region WhereBetween Tests

    [TestMethod]
    public void QueryBuilder_WhereBetween_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .WhereBetween(PersonMeta.Age, 28, 33);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(5, results); // Bob (30), Diana (28), Eve (32), Ivy (29), Jack (33)
    }

    [TestMethod]
    public void QueryBuilder_WhereBetween_IncludesEdges()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .WhereBetween(PersonMeta.Age, 25, 25);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(1, results);
        Assert.AreEqual("Alice", results[0].Name);
    }

    #endregion

    #region WhereIn Tests

    [TestMethod]
    public void QueryBuilder_WhereIn_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .WhereIn(PersonMeta.Name, "Alice", "Bob", "Charlie");

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(3, results);
    }

    [TestMethod]
    public void QueryBuilder_WhereIn_WithIntValues_FiltersCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .WhereIn(PersonMeta.Age, 25, 30, 35);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(3, results); // Alice (25), Bob (30), Charlie (35)
    }

    #endregion

    #region Multiple Filters Tests

    [TestMethod]
    public void QueryBuilder_MultipleFilters_AppliedWithAnd()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
            .Where(PersonMeta.Email, FieldOp.EndsWith, "@example.com");

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(4, results); // Bob (30), Frank (45), Henry (38), Jack (33)
    }

    [TestMethod]
    public void QueryBuilder_CombinedFilters_WorkCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .WhereBetween(PersonMeta.Age, 25, 35)
            .Where(PersonMeta.Email, FieldOp.Contains, "example");

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(4, results); // Alice (25), Bob (30), Diana (28), Jack (33)
    }

    #endregion

    #region Pagination Tests

    [TestMethod]
    public void QueryBuilder_Limit_ReturnsSpecifiedCount()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Limit(3);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(3, results);
    }

    [TestMethod]
    public void QueryBuilder_Skip_SkipsSpecifiedCount()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Skip(5);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(5, results);
        Assert.AreEqual("Frank", results[0].Name); // First after skipping 5
    }

    [TestMethod]
    public void QueryBuilder_SkipAndLimit_WorkTogether()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Skip(2)
            .Limit(3);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(3, results);
        Assert.AreEqual("Charlie", results[0].Name);
        Assert.AreEqual("Diana", results[1].Name);
        Assert.AreEqual("Eve", results[2].Name);
    }

    [TestMethod]
    public void QueryBuilder_SkipAndLimit_WithFilter()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 25)
            .Skip(2)
            .Limit(2);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(2, results);
    }

    #endregion

    #region FirstOrDefault Tests

    [TestMethod]
    public void QueryBuilder_FirstOrDefault_ReturnsFirstMatch()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.StartsWith, "A");

        // Act
        Person result = query.FirstOrDefault();

        // Assert
        Assert.IsNotNull(result);
        Assert.AreEqual("Alice", result.Name);
    }

    [TestMethod]
    public void QueryBuilder_FirstOrDefault_ReturnsNullWhenNoMatch()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent");

        // Act
        Person result = query.FirstOrDefault();

        // Assert
        Assert.IsNull(result);
    }

    #endregion

    #region Count Tests

    [TestMethod]
    public void QueryBuilder_Count_ReturnsCorrectCount()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 30);

        // Act
        int count = query.Count();

        // Assert
        Assert.AreEqual(5, count); // Eve (32), Charlie (35), Frank (45), Henry (38), Jack (33)
    }

    [TestMethod]
    public void QueryBuilder_Count_WithNoFilters_ReturnsTotalCount()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor);

        // Act
        int count = query.Count();

        // Assert
        Assert.AreEqual(10, count);
    }

    [TestMethod]
    public void QueryBuilder_Count_ReturnsZeroWhenNoMatch()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 100);

        // Act
        int count = query.Count();

        // Assert
        Assert.AreEqual(0, count);
    }

    #endregion

    #region Fluent API Tests

    [TestMethod]
    public void QueryBuilder_FluentApi_Chainable()
    {
        // Arrange & Act
        List<Person> results = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 25)
            .Where(PersonMeta.Age, FieldOp.LessThanOrEqual, 35)
            .Where(PersonMeta.Email, FieldOp.Contains, "@")
            .Skip(1)
            .Limit(5)
            .ToList();

        // Assert
        Assert.IsLessThanOrEqualTo(5, results.Count);
    }

    [TestMethod]
    public void QueryBuilder_Properties_ExposeCorrectValues()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.Equals, "Alice")
            .WhereBetween(PersonMeta.Age, 20, 30)
            .Skip(5)
            .Limit(10);

        // Assert
        Assert.HasCount(2, query.Filters);
        Assert.AreEqual(5, query.SkipValue);
        Assert.AreEqual(10, query.LimitValue);
    }

    #endregion

    #region Edge Cases

    [TestMethod]
    public void QueryBuilder_EmptyDataSet_ReturnsEmptyList()
    {
        // Arrange
        InMemoryQueryExecutor<Person> emptyExecutor = new InMemoryQueryExecutor<Person>(new List<Person>());
        QueryBuilder<Person> query = new QueryBuilder<Person>(emptyExecutor);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void QueryBuilder_NoFilters_ReturnsAllDocuments()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.HasCount(10, results);
    }

    [TestMethod]
    public void QueryBuilder_SkipMoreThanAvailable_ReturnsEmptyList()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Skip(100);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.IsEmpty(results);
    }

    [TestMethod]
    public void QueryBuilder_LimitZero_ReturnsEmptyList()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Limit(0);

        // Act
        List<Person> results = query.ToList();

        // Assert
        Assert.IsEmpty(results);
    }

    #endregion

    #region Any Tests

    [TestMethod]
    public void QueryBuilder_Any_WithMatches_ReturnsTrue()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 30);

        // Act
        bool result = query.Any();

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void QueryBuilder_Any_WithNoMatches_ReturnsFalse()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThan, 100);

        // Act
        bool result = query.Any();

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void QueryBuilder_Any_WithNoFilters_ReturnsTrueWhenDataExists()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor);

        // Act
        bool result = query.Any();

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public void QueryBuilder_Any_EmptyDataSet_ReturnsFalse()
    {
        // Arrange
        InMemoryQueryExecutor<Person> emptyExecutor = new InMemoryQueryExecutor<Person>(new List<Person>());
        QueryBuilder<Person> query = new QueryBuilder<Person>(emptyExecutor);

        // Act
        bool result = query.Any();

        // Assert
        Assert.IsFalse(result);
    }

    [TestMethod]
    public void QueryBuilder_Any_MultipleFilters_WorksCorrectly()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
            .Where(PersonMeta.Email, FieldOp.EndsWith, "@test.com");

        // Act
        bool result = query.Any();

        // Assert
        Assert.IsTrue(result); // Eve (32, @test.com)
    }

    [TestMethod]
    public async Task QueryBuilder_AnyAsync_WithMatches_ReturnsTrue()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.StartsWith, "A");

        // Act
        bool result = await query.AnyAsync();

        // Assert
        Assert.IsTrue(result);
    }

    [TestMethod]
    public async Task QueryBuilder_AnyAsync_WithNoMatches_ReturnsFalse()
    {
        // Arrange
        QueryBuilder<Person> query = new QueryBuilder<Person>(_executor)
            .Where(PersonMeta.Name, FieldOp.Equals, "NonExistent");

        // Act
        bool result = await query.AnyAsync();

        // Assert
        Assert.IsFalse(result);
    }

    #endregion
}
