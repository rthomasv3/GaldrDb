using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class IntegrationTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbIntegrationTests_{Guid.NewGuid()}");
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

    [TestMethod]
    public void BasicWorkflow_CreateInsertQueryCloseReopenQuery_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int insertedDocId;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "John Doe", Age = 30, Email = "john@example.com" };
            insertedDocId = db.InsertDocument("people", person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved = db.GetDocument<Person>("people", insertedDocId);

            bool result = retrieved != null &&
                          retrieved.Name == "John Doe" &&
                          retrieved.Age == 30 &&
                          retrieved.Email == "john@example.com";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void MultipleDocuments_InsertAndQueryAll_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            Person person1 = new Person { Name = "Alice", Age = 25, Email = "alice@example.com" };
            Person person2 = new Person { Name = "Bob", Age = 35, Email = "bob@example.com" };
            Person person3 = new Person { Name = "Charlie", Age = 45, Email = "charlie@example.com" };

            int id1 = db.InsertDocument("people", person1);
            int id2 = db.InsertDocument("people", person2);
            int id3 = db.InsertDocument("people", person3);

            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);
            Person retrieved3 = db.GetDocument<Person>("people", id3);

            bool result = retrieved1.Name == "Alice" &&
                          retrieved2.Name == "Bob" &&
                          retrieved3.Name == "Charlie";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void MultipleCollections_InsertAndQueryAcrossCollections_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("employees");
            db.CreateCollection("customers");

            Person employee = new Person { Name = "Employee One", Age = 28, Email = "emp@company.com" };
            Person customer = new Person { Name = "Customer One", Age = 40, Email = "cust@email.com" };

            int empId = db.InsertDocument("employees", employee);
            int custId = db.InsertDocument("customers", customer);

            Person retrievedEmp = db.GetDocument<Person>("employees", empId);
            Person retrievedCust = db.GetDocument<Person>("customers", custId);

            bool result = retrievedEmp.Name == "Employee One" &&
                          retrievedCust.Name == "Customer One";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void BulkInsert_Insert100Documents_AllRetrievable()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int[] docIds = new int[100];

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            for (int i = 0; i < 100; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i}",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@example.com"
                };
                docIds[i] = db.InsertDocument("people", person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            bool allFound = true;
            for (int i = 0; i < 100; i++)
            {
                Person retrieved = db.GetDocument<Person>("people", docIds[i]);
                if (retrieved == null || retrieved.Name != $"Person {i}")
                {
                    allFound = false;
                    break;
                }
            }

            Assert.IsTrue(allFound);
        }
    }

    [TestMethod]
    public void Persistence_DataSurvivesMultipleOpenClose_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        int id1;
        int id2;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");
            Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
            id1 = db.InsertDocument("people", person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person person2 = new Person { Name = "Second", Age = 30, Email = "second@example.com" };
            id2 = db.InsertDocument("people", person2);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
        {
            Person retrieved1 = db.GetDocument<Person>("people", id1);
            Person retrieved2 = db.GetDocument<Person>("people", id2);

            bool result = retrieved1.Name == "Original" && retrieved2.Name == "Second";

            Assert.IsTrue(result);
        }
    }

    [TestMethod]
    public void Error_CreateCollection_AlreadyExists_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            try
            {
                db.CreateCollection("people");
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void Error_InsertDocument_CollectionNotFound_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            try
            {
                Person person = new Person { Name = "Test", Age = 25, Email = "test@example.com" };
                db.InsertDocument("nonexistent", person);
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void Error_GetDocument_CollectionNotFound_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            try
            {
                Person result = db.GetDocument<Person>("nonexistent", 0);
            }
            catch (InvalidOperationException)
            {
                exceptionThrown = true;
            }
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void Error_GetDocument_DocumentNotFound_ReturnsDefault()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.CreateCollection("people");

            Person result = db.GetDocument<Person>("people", 999);

            Assert.IsNull(result);
        }
    }

    [TestMethod]
    public void FileExpansion_HeavyInserts_ExpansionOccurs()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        long initialSize;
        long finalSize;

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            initialSize = new FileInfo(dbPath).Length;

            db.CreateCollection("people");

            for (int i = 0; i < 500; i++)
            {
                Person person = new Person
                {
                    Name = $"Person {i} with a much longer name to take up more space in the database file and force expansion",
                    Age = 20 + i,
                    Email = $"person{i}@example.com"
                };
                db.InsertDocument("people", person);
            }
        }

        finalSize = new FileInfo(dbPath).Length;

        bool result = finalSize > initialSize;

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void Error_CreateDatabase_FileAlreadyExists_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "test.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        bool exceptionThrown = false;

        using (GaldrDbInstance db1 = GaldrDbInstance.Create(dbPath, options))
        {
        }

        try
        {
            using (GaldrDbInstance db2 = GaldrDbInstance.Create(dbPath, options))
            {
            }
        }
        catch (InvalidOperationException)
        {
            exceptionThrown = true;
        }

        Assert.IsTrue(exceptionThrown);
    }

    [TestMethod]
    public void Error_OpenDatabase_FileNotFound_ThrowsException()
    {
        string dbPath = Path.Combine(_testDirectory, "nonexistent.db");

        bool exceptionThrown = false;

        try
        {
            using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath))
            {
            }
        }
        catch (FileNotFoundException)
        {
            exceptionThrown = true;
        }

        Assert.IsTrue(exceptionThrown);
    }
}
