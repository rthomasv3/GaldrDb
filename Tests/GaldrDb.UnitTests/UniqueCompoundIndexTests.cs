using System;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class UniqueCompoundIndexTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbUniqueCompoundTests_{Guid.NewGuid()}");
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

    private GaldrDbInstance CreateDatabase(string dbName)
    {
        string dbPath = Path.Combine(_testDirectory, dbName);
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false, UseMmap = false };
        return GaldrDbInstance.Create(dbPath, options);
    }

    [TestMethod]
    public void Insert_UniqueCompoundIndex_FirstInsert_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_first.db"))
        {
            Employee emp = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "ENG001",
                HireDate = new DateTime(2024, 1, 15)
            };

            int id = db.Insert(emp);

            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void Insert_UniqueCompoundIndex_DifferentDepartmentSameNumber_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_diff_dept.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Marketing",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 2, 1)
            };
            int id2 = db.Insert(emp2);

            Assert.AreEqual(2, id2);
        }
    }

    [TestMethod]
    public void Insert_UniqueCompoundIndex_SameDepartmentDifferentNumber_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_diff_num.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "002",
                HireDate = new DateTime(2024, 2, 1)
            };
            int id2 = db.Insert(emp2);

            Assert.AreEqual(2, id2);
        }
    }

    [TestMethod]
    public void Insert_UniqueCompoundIndex_DuplicateCompoundKey_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_dup.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 2, 1)
            };

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Insert(emp2);
            });

            Assert.Contains("Unique constraint violation", exception.Message);
        }
    }

    [TestMethod]
    public void Update_UniqueCompoundIndex_SameValues_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_update_same.db"))
        {
            Employee emp = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            int id = db.Insert(emp);

            emp.Name = "Alice Smith";
            bool updated = db.Replace(emp);

            Assert.IsTrue(updated);

            Employee retrieved = db.GetById<Employee>(id);
            Assert.AreEqual("Alice Smith", retrieved.Name);
            Assert.AreEqual("Engineering", retrieved.Department);
            Assert.AreEqual("001", retrieved.EmployeeNumber);
        }
    }

    [TestMethod]
    public void Update_UniqueCompoundIndex_ChangeToExistingCompoundKey_ThrowsException()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_update_dup.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "002",
                HireDate = new DateTime(2024, 2, 1)
            };
            db.Insert(emp2);

            emp2.EmployeeNumber = "001";

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Replace(emp2);
            });

            Assert.Contains("Unique constraint violation", exception.Message);
        }
    }

    [TestMethod]
    public void Update_UniqueCompoundIndex_ChangeToNewCompoundKey_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_update_new.db"))
        {
            Employee emp = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            int id = db.Insert(emp);

            emp.Department = "Research";
            bool updated = db.Replace(emp);

            Assert.IsTrue(updated);

            Employee retrieved = db.GetById<Employee>(id);
            Assert.AreEqual("Research", retrieved.Department);
        }
    }

    [TestMethod]
    public void Delete_UniqueCompoundIndex_ThenInsertSameKey_Succeeds()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_compound_delete_reuse.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            int id1 = db.Insert(emp1);

            db.DeleteById<Employee>(id1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 2, 1)
            };
            int id2 = db.Insert(emp2);

            Assert.AreEqual(2, id2);

            Employee retrieved = db.GetById<Employee>(id2);
            Assert.AreEqual("Bob", retrieved.Name);
        }
    }

    [TestMethod]
    public void UniqueCompoundIndex_NonUniqueCompoundIndex_BothWork()
    {
        using (GaldrDbInstance db = CreateDatabase("unique_and_nonunique.db"))
        {
            Employee emp1 = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp1);

            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "002",
                HireDate = new DateTime(2024, 1, 15)
            };
            int id2 = db.Insert(emp2);

            Assert.AreEqual(2, id2);
        }
    }

    [TestMethod]
    public void UniqueCompoundIndex_PersistsAcrossReopen()
    {
        string dbPath = Path.Combine(_testDirectory, "persist_unique.db");

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            Employee emp = new Employee
            {
                Name = "Alice",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 1, 15)
            };
            db.Insert(emp);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, new GaldrDbOptions { UseWal = false, UseMmap = false }))
        {
            Employee emp2 = new Employee
            {
                Name = "Bob",
                Department = "Engineering",
                EmployeeNumber = "001",
                HireDate = new DateTime(2024, 2, 1)
            };

            InvalidOperationException exception = Assert.ThrowsExactly<InvalidOperationException>(() =>
            {
                db.Insert(emp2);
            });

            Assert.Contains("Unique constraint violation", exception.Message);
        }
    }
}
