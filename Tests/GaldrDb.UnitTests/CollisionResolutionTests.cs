using System;
using System.Collections.Generic;
using System.Linq;
using GaldrDb.UnitTests.TestModels;
using GaldrDb.UnitTests.TestModels.CompanyA.Models;
using GaldrDb.UnitTests.TestModels.CompanyB.Models;
using GaldrDb.UnitTests.TestModels.Customers;
using GaldrDb.UnitTests.TestModels.Products;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.UnitTests;

[TestClass]
public class CollisionResolutionTests
{
    #region Simple Collision Tests (One Namespace Segment)

    [TestMethod]
    public void CustomersOrder_CollectionName_IncludesNamespaceSegment()
    {
        // Act
        string collectionName = CustomersOrderMeta.CollectionName;

        // Assert
        Assert.AreEqual("CustomersOrder", collectionName);
    }

    [TestMethod]
    public void ProductsOrder_CollectionName_IncludesNamespaceSegment()
    {
        // Act
        string collectionName = ProductsOrderMeta.CollectionName;

        // Assert
        Assert.AreEqual("ProductsOrder", collectionName);
    }

    [TestMethod]
    public void CustomersOrder_And_ProductsOrder_HaveDifferentCollectionNames()
    {
        // Act
        string customersCollectionName = CustomersOrderMeta.CollectionName;
        string productsCollectionName = ProductsOrderMeta.CollectionName;

        // Assert
        Assert.AreNotEqual(customersCollectionName, productsCollectionName);
    }

    [TestMethod]
    public void CustomersOrder_FieldAccessor_WorksCorrectly()
    {
        // Arrange
        GaldrDb.UnitTests.TestModels.Customers.Order order = new GaldrDb.UnitTests.TestModels.Customers.Order
        {
            Id = 1,
            CustomerId = 42,
            TotalAmount = 99.99m
        };

        // Act
        int id = CustomersOrderMeta.Id.Accessor(order);
        int customerId = CustomersOrderMeta.CustomerId.Accessor(order);
        decimal totalAmount = CustomersOrderMeta.TotalAmount.Accessor(order);

        // Assert
        Assert.AreEqual(1, id);
        Assert.AreEqual(42, customerId);
        Assert.AreEqual(99.99m, totalAmount);
    }

    [TestMethod]
    public void ProductsOrder_FieldAccessor_WorksCorrectly()
    {
        // Arrange
        GaldrDb.UnitTests.TestModels.Products.Order order = new GaldrDb.UnitTests.TestModels.Products.Order
        {
            Id = 2,
            ProductId = 100,
            Quantity = 5
        };

        // Act
        int id = ProductsOrderMeta.Id.Accessor(order);
        int productId = ProductsOrderMeta.ProductId.Accessor(order);
        int quantity = ProductsOrderMeta.Quantity.Accessor(order);

        // Assert
        Assert.AreEqual(2, id);
        Assert.AreEqual(100, productId);
        Assert.AreEqual(5, quantity);
    }

    [TestMethod]
    public void CustomersOrder_IndexedFields_ContainsCustomerId()
    {
        // Act
        IReadOnlyList<string> indexedFields = CustomersOrderMeta.IndexedFieldNames;

        // Assert
        Assert.IsTrue(indexedFields.Contains("CustomerId"));
    }

    [TestMethod]
    public void ProductsOrder_IndexedFields_ContainsProductId()
    {
        // Act
        IReadOnlyList<string> indexedFields = ProductsOrderMeta.IndexedFieldNames;

        // Assert
        Assert.IsTrue(indexedFields.Contains("ProductId"));
    }

    #endregion

    #region Deep Collision Tests (Multiple Namespace Segments)

    [TestMethod]
    public void CompanyAEntity_CollectionName_IncludesMultipleNamespaceSegments()
    {
        // Act
        string collectionName = CompanyAModelsEntityMeta.CollectionName;

        // Assert
        Assert.AreEqual("CompanyAModelsEntity", collectionName);
    }

    [TestMethod]
    public void CompanyBEntity_CollectionName_IncludesMultipleNamespaceSegments()
    {
        // Act
        string collectionName = CompanyBModelsEntityMeta.CollectionName;

        // Assert
        Assert.AreEqual("CompanyBModelsEntity", collectionName);
    }

    [TestMethod]
    public void CompanyAEntity_And_CompanyBEntity_HaveDifferentCollectionNames()
    {
        // Act
        string companyACollectionName = CompanyAModelsEntityMeta.CollectionName;
        string companyBCollectionName = CompanyBModelsEntityMeta.CollectionName;

        // Assert
        Assert.AreNotEqual(companyACollectionName, companyBCollectionName);
    }

    [TestMethod]
    public void CompanyAEntity_FieldAccessor_WorksCorrectly()
    {
        // Arrange
        GaldrDb.UnitTests.TestModels.CompanyA.Models.Entity entity = new GaldrDb.UnitTests.TestModels.CompanyA.Models.Entity
        {
            Id = 10,
            CompanyAData = "Data from Company A"
        };

        // Act
        int id = CompanyAModelsEntityMeta.Id.Accessor(entity);
        string data = CompanyAModelsEntityMeta.CompanyAData.Accessor(entity);

        // Assert
        Assert.AreEqual(10, id);
        Assert.AreEqual("Data from Company A", data);
    }

    [TestMethod]
    public void CompanyBEntity_FieldAccessor_WorksCorrectly()
    {
        // Arrange
        GaldrDb.UnitTests.TestModels.CompanyB.Models.Entity entity = new GaldrDb.UnitTests.TestModels.CompanyB.Models.Entity
        {
            Id = 20,
            CompanyBData = "Data from Company B"
        };

        // Act
        int id = CompanyBModelsEntityMeta.Id.Accessor(entity);
        string data = CompanyBModelsEntityMeta.CompanyBData.Accessor(entity);

        // Assert
        Assert.AreEqual(20, id);
        Assert.AreEqual("Data from Company B", data);
    }

    #endregion

    #region GaldrCollection Override Tests

    [TestMethod]
    public void LegacyCustomer_CollectionName_UsesOverride()
    {
        // Act
        string collectionName = LegacyCustomerMeta.CollectionName;

        // Assert - uses the [GaldrCollection("Customer")] override, not "LegacyCustomer"
        Assert.AreEqual("Customer", collectionName);
    }

    [TestMethod]
    public void LegacyCustomer_MetaClassName_DoesNotUseOverride()
    {
        // The meta class should still be named after the actual class, not the collection override
        // This test verifies by checking that LegacyCustomerMeta exists and works
        
        // Act
        GaldrTypeInfo<LegacyCustomer> typeInfo = LegacyCustomerMeta.TypeInfo;

        // Assert
        Assert.IsNotNull(typeInfo);
        Assert.AreEqual("Customer", typeInfo.CollectionName);
    }

    [TestMethod]
    public void LegacyCustomer_FieldAccessor_WorksCorrectly()
    {
        // Arrange
        LegacyCustomer customer = new LegacyCustomer
        {
            Id = 100,
            Name = "Acme Corp",
            Email = "contact@acme.com"
        };

        // Act
        int id = LegacyCustomerMeta.Id.Accessor(customer);
        string name = LegacyCustomerMeta.Name.Accessor(customer);
        string email = LegacyCustomerMeta.Email.Accessor(customer);

        // Assert
        Assert.AreEqual(100, id);
        Assert.AreEqual("Acme Corp", name);
        Assert.AreEqual("contact@acme.com", email);
    }

    [TestMethod]
    public void LegacyCustomer_IndexedFields_ContainsName()
    {
        // Act
        IReadOnlyList<string> indexedFields = LegacyCustomerMeta.IndexedFieldNames;

        // Assert
        Assert.IsTrue(indexedFields.Contains("Name"));
    }

    #endregion

    #region Registry Tests

    [TestMethod]
    public void Registry_ContainsAllTypes()
    {
        // Act
        IEnumerable<IGaldrTypeInfo> allTypes = GaldrTypeRegistry.GetAll();
        List<string> collectionNames = allTypes.Select(t => t.CollectionName).ToList();

        // Assert - should have all 6 types
        Assert.Contains("Person", collectionNames);
        Assert.Contains("CustomersOrder", collectionNames);
        Assert.Contains("ProductsOrder", collectionNames);
        Assert.Contains("CompanyAModelsEntity", collectionNames);
        Assert.Contains("CompanyBModelsEntity", collectionNames);
        Assert.Contains("Customer", collectionNames); // LegacyCustomer with override
    }

    [TestMethod]
    public void Registry_Get_CustomersOrder_ReturnsCorrectType()
    {
        // Act
        GaldrTypeInfo<GaldrDb.UnitTests.TestModels.Customers.Order> typeInfo = 
            GaldrTypeRegistry.Get<GaldrDb.UnitTests.TestModels.Customers.Order>();

        // Assert
        Assert.AreEqual("CustomersOrder", typeInfo.CollectionName);
    }

    [TestMethod]
    public void Registry_Get_ProductsOrder_ReturnsCorrectType()
    {
        // Act
        GaldrTypeInfo<GaldrDb.UnitTests.TestModels.Products.Order> typeInfo = 
            GaldrTypeRegistry.Get<GaldrDb.UnitTests.TestModels.Products.Order>();

        // Assert
        Assert.AreEqual("ProductsOrder", typeInfo.CollectionName);
    }

    [TestMethod]
    public void Registry_Get_CompanyAEntity_ReturnsCorrectType()
    {
        // Act
        GaldrTypeInfo<GaldrDb.UnitTests.TestModels.CompanyA.Models.Entity> typeInfo = 
            GaldrTypeRegistry.Get<GaldrDb.UnitTests.TestModels.CompanyA.Models.Entity>();

        // Assert
        Assert.AreEqual("CompanyAModelsEntity", typeInfo.CollectionName);
    }

    [TestMethod]
    public void Registry_Get_CompanyBEntity_ReturnsCorrectType()
    {
        // Act
        GaldrTypeInfo<GaldrDb.UnitTests.TestModels.CompanyB.Models.Entity> typeInfo = 
            GaldrTypeRegistry.Get<GaldrDb.UnitTests.TestModels.CompanyB.Models.Entity>();

        // Assert
        Assert.AreEqual("CompanyBModelsEntity", typeInfo.CollectionName);
    }

    [TestMethod]
    public void Registry_Get_LegacyCustomer_ReturnsCorrectType()
    {
        // Act
        GaldrTypeInfo<LegacyCustomer> typeInfo = GaldrTypeRegistry.Get<LegacyCustomer>();

        // Assert
        Assert.AreEqual("Customer", typeInfo.CollectionName);
    }

    [TestMethod]
    public void Registry_TryGet_UnknownType_ReturnsFalse()
    {
        // Act
        bool found = GaldrTypeRegistry.TryGet<string>(out GaldrTypeInfo<string> typeInfo);

        // Assert
        Assert.IsFalse(found);
        Assert.IsNull(typeInfo);
    }

    [TestMethod]
    public void Registry_GetByType_WorksForCollidingTypes()
    {
        // Act
        IGaldrTypeInfo customersOrderInfo = GaldrTypeRegistry.Get(typeof(GaldrDb.UnitTests.TestModels.Customers.Order));
        IGaldrTypeInfo productsOrderInfo = GaldrTypeRegistry.Get(typeof(GaldrDb.UnitTests.TestModels.Products.Order));

        // Assert
        Assert.AreEqual("CustomersOrder", customersOrderInfo.CollectionName);
        Assert.AreEqual("ProductsOrder", productsOrderInfo.CollectionName);
    }

    #endregion
}
