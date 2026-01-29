using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Query;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDbInstance = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class EncryptionTests
{
    private string _testDirectory;

    [TestInitialize]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbEncryptionTests_{Guid.NewGuid()}");
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

    #region EncryptedPageIO Tests

    [TestMethod]
    public void EncryptedPageIO_CreateAndWritePage_Success()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        int usableSize = pageSize - PageConstants.ENCRYPTION_RESERVE_SIZE;
        EncryptionOptions encOptions = new EncryptionOptions { Password = "testPassword123", KdfIterations = 1000 };

        byte[] testData = new byte[pageSize];
        for (int i = 0; i < usableSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }
        // Last 32 bytes are reserved for encryption overhead

        using (EncryptedPageIO pageIO = EncryptedPageIO.Create(filePath, pageSize, encOptions, out _))
        {
            pageIO.WritePage(0, testData);
            pageIO.Flush();

            byte[] readData = new byte[pageSize];
            pageIO.ReadPage(0, readData);

            // Only compare usable portion
            for (int i = 0; i < usableSize; i++)
            {
                Assert.AreEqual(testData[i], readData[i], $"Mismatch at index {i}");
            }
        }

        bool result = File.Exists(filePath);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void EncryptedPageIO_WriteMultiplePages_Success()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        int usableSize = pageSize - PageConstants.ENCRYPTION_RESERVE_SIZE;
        EncryptionOptions encOptions = new EncryptionOptions { Password = "testPassword123", KdfIterations = 1000 };

        using (EncryptedPageIO pageIO = EncryptedPageIO.Create(filePath, pageSize, encOptions, out _))
        {
            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] testData = new byte[pageSize];
                for (int i = 0; i < usableSize; i++)
                {
                    testData[i] = (byte)((pageId * 100 + i) % 256);
                }
                pageIO.WritePage(pageId, testData);
            }

            pageIO.Flush();

            for (int pageId = 0; pageId < 10; pageId++)
            {
                byte[] expectedData = new byte[pageSize];
                for (int i = 0; i < usableSize; i++)
                {
                    expectedData[i] = (byte)((pageId * 100 + i) % 256);
                }

                byte[] readData = new byte[pageSize];
                pageIO.ReadPage(pageId, readData);

                // Only compare usable portion
                for (int i = 0; i < usableSize; i++)
                {
                    Assert.AreEqual(expectedData[i], readData[i], $"Page {pageId}, index {i} mismatch");
                }
            }
        }
    }

    [TestMethod]
    public void EncryptedPageIO_OpenWithCorrectPassword_Success()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        int usableSize = pageSize - PageConstants.ENCRYPTION_RESERVE_SIZE;
        string password = "correctPassword";
        EncryptionOptions encOptions = new EncryptionOptions { Password = password, KdfIterations = 1000 };

        byte[] testData = new byte[pageSize];
        // Write MAGIC_NUMBER at start so password validation passes
        BinaryHelper.WriteUInt32LE(testData, 0, PageConstants.MAGIC_NUMBER);
        for (int i = 4; i < usableSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }
        // Last 32 bytes are reserved for encryption overhead, leave as zeros

        using (EncryptedPageIO createIO = EncryptedPageIO.Create(filePath, pageSize, encOptions, out _))
        {
            createIO.WritePage(0, testData);
            createIO.Flush();
        }

        using (EncryptedPageIO openIO = EncryptedPageIO.Open(filePath, pageSize, encOptions, out _))
        {
            byte[] readData = new byte[pageSize];
            openIO.ReadPage(0, readData);

            // Only compare usable portion - last 32 bytes are encryption overhead
            for (int i = 0; i < usableSize; i++)
            {
                Assert.AreEqual(testData[i], readData[i], $"Mismatch at index {i}");
            }
        }
    }

    [TestMethod]
    public void EncryptedPageIO_OpenWithWrongPassword_ThrowsInvalidPasswordException()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        EncryptionOptions createOptions = new EncryptionOptions { Password = "correctPassword", KdfIterations = 1000 };
        EncryptionOptions wrongOptions = new EncryptionOptions { Password = "wrongPassword", KdfIterations = 1000 };

        byte[] testData = new byte[pageSize];
        BinaryHelper.WriteUInt32LE(testData, 0, PageConstants.MAGIC_NUMBER);
        for (int i = 4; i < pageSize; i++)
        {
            testData[i] = (byte)(i % 256);
        }

        using (EncryptedPageIO createIO = EncryptedPageIO.Create(filePath, pageSize, createOptions, out _))
        {
            createIO.WritePage(0, testData);
            createIO.Flush();
        }

        Assert.Throws<InvalidPasswordException>(() =>
        {
            using (EncryptedPageIO openIO = EncryptedPageIO.Open(filePath, pageSize, wrongOptions, out _))
            {
            }
        });
    }

    [TestMethod]
    public void EncryptedPageIO_IsEncryptedFile_ReturnsTrueForEncrypted()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        EncryptionOptions encOptions = new EncryptionOptions { Password = "testPassword", KdfIterations = 1000 };

        using (EncryptedPageIO pageIO = EncryptedPageIO.Create(filePath, pageSize, encOptions, out _))
        {
            pageIO.Flush();
        }

        bool result = EncryptedPageIO.IsEncryptedFile(filePath);

        Assert.IsTrue(result);
    }

    [TestMethod]
    public void EncryptedPageIO_IsEncryptedFile_ReturnsFalseForUnencrypted()
    {
        string filePath = Path.Combine(_testDirectory, "unencrypted.db");
        GaldrDbOptions options = new GaldrDbOptions { PageSize = 8192, UseWal = false };

        using (GaldrDbInstance db = GaldrDbInstance.Create(filePath, options))
        {
        }

        bool result = EncryptedPageIO.IsEncryptedFile(filePath);

        Assert.IsFalse(result);
    }

    [TestMethod]
    public void EncryptedPageIO_GetPageSize_ReturnsCorrectSize()
    {
        string filePath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        EncryptionOptions encOptions = new EncryptionOptions { Password = "testPassword", KdfIterations = 1000 };

        using (EncryptedPageIO pageIO = EncryptedPageIO.Create(filePath, pageSize, encOptions, out _))
        {
            pageIO.Flush();
        }

        int result = EncryptedPageIO.GetPageSize(filePath);

        Assert.AreEqual(pageSize, result);
    }

    #endregion

    #region Database-Level Encryption Tests (Without WAL)

    [TestMethod]
    public void EncryptedDatabase_CreateAndInsert_WithoutWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "dbPassword123", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Alice", Age = 30, Email = "alice@example.com" };
            int id = db.Insert(person);

            Assert.AreEqual(1, id);

            Person retrieved = db.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Alice", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_ReopenWithCorrectPassword_WithoutWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "dbPassword123", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Bob", Age = 25, Email = "bob@example.com" };
            db.Insert(person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(1);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Bob", retrieved.Name);
            Assert.AreEqual(25, retrieved.Age);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_OpenWithWrongPassword_ThrowsInvalidPasswordException()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions createOptions = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "correctPassword", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, createOptions))
        {
            Person person = new Person { Name = "Test", Age = 20, Email = "test@example.com" };
            db.Insert(person);
        }

        GaldrDbOptions wrongOptions = new GaldrDbOptions
        {
            Encryption = new EncryptionOptions { Password = "wrongPassword", KdfIterations = 1000 }
        };

        Assert.Throws<InvalidPasswordException>(() =>
        {
            using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, wrongOptions))
            {
            }
        });
    }

    [TestMethod]
    public void EncryptedDatabase_OpenWithoutPassword_ThrowsInvalidPasswordException()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions createOptions = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "secretPassword", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, createOptions))
        {
            Person person = new Person { Name = "Test", Age = 20, Email = "test@example.com" };
            db.Insert(person);
        }

        GaldrDbOptions noPasswordOptions = new GaldrDbOptions();

        Assert.Throws<InvalidPasswordException>(() =>
        {
            using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, noPasswordOptions))
            {
            }
        });
    }

    [TestMethod]
    public void UnencryptedDatabase_OpenWithPassword_ThrowsInvalidOperationException()
    {
        string dbPath = Path.Combine(_testDirectory, "unencrypted.db");
        GaldrDbOptions createOptions = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, createOptions))
        {
            Person person = new Person { Name = "Test", Age = 20, Email = "test@example.com" };
            db.Insert(person);
        }

        GaldrDbOptions passwordOptions = new GaldrDbOptions
        {
            Encryption = new EncryptionOptions { Password = "unnecessary", KdfIterations = 1000 }
        };

        Assert.Throws<InvalidOperationException>(() =>
        {
            using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, passwordOptions))
            {
            }
        });
    }

    #endregion

    #region Database-Level Encryption Tests (With WAL)

    [TestMethod]
    public void EncryptedDatabase_CreateAndInsert_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "walPassword123", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" };
            int id = db.Insert(person);

            Assert.AreEqual(1, id);

            Person retrieved = db.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Charlie", retrieved.Name);
            Assert.AreEqual(35, retrieved.Age);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_ReopenWithCorrectPassword_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "walPassword123", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Diana", Age = 28, Email = "diana@example.com" };
            db.Insert(person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(1);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Diana", retrieved.Name);
            Assert.AreEqual(28, retrieved.Age);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_MultipleInserts_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "multiInsertPass", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 100; i++)
            {
                Person person = new Person
                {
                    Name = $"Person{i}",
                    Age = 20 + (i % 50),
                    Email = $"person{i}@example.com"
                };
                db.Insert(person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 1; i <= 100; i++)
            {
                Person retrieved = db.GetById<Person>(i);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual($"Person{i - 1}", retrieved.Name);
            }
        }
    }

    #endregion

    #region Checkpoint Tests

    [TestMethod]
    public void EncryptedDatabase_Checkpoint_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            AutoCheckpoint = false,
            Encryption = new EncryptionOptions { Password = "checkpointPass", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 50; i++)
            {
                Person person = new Person
                {
                    Name = $"CheckpointPerson{i}",
                    Age = 25 + i,
                    Email = $"cp{i}@example.com"
                };
                db.Insert(person);
            }

            db.Checkpoint();

            Person retrieved = db.GetById<Person>(25);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("CheckpointPerson24", retrieved.Name);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CheckpointAndReopen_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            AutoCheckpoint = false,
            Encryption = new EncryptionOptions { Password = "checkpointReopenPass", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 20; i++)
            {
                Person person = new Person
                {
                    Name = $"BeforeCheckpoint{i}",
                    Age = 30 + i,
                    Email = $"bc{i}@example.com"
                };
                db.Insert(person);
            }

            db.Checkpoint();

            for (int i = 0; i < 10; i++)
            {
                Person person = new Person
                {
                    Name = $"AfterCheckpoint{i}",
                    Age = 50 + i,
                    Email = $"ac{i}@example.com"
                };
                db.Insert(person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person beforeCheckpoint = db.GetById<Person>(10);
            Person afterCheckpoint = db.GetById<Person>(25);

            Assert.IsNotNull(beforeCheckpoint);
            Assert.AreEqual("BeforeCheckpoint9", beforeCheckpoint.Name);

            Assert.IsNotNull(afterCheckpoint);
            Assert.AreEqual("AfterCheckpoint4", afterCheckpoint.Name);
        }
    }

    #endregion

    #region Data Confidentiality Tests

    [TestMethod]
    public void EncryptedDatabase_FileDoesNotContainPlaintext()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        string uniqueString = "UNIQUE_SEARCHABLE_STRING_12345";
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "confidentialityTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person
            {
                Name = uniqueString,
                Age = 99,
                Email = $"{uniqueString}@example.com"
            };
            db.Insert(person);
        }

        byte[] fileContents = File.ReadAllBytes(dbPath);
        string fileAsString = Encoding.UTF8.GetString(fileContents);

        bool containsPlaintext = fileAsString.Contains(uniqueString);

        Assert.IsFalse(containsPlaintext, "Encrypted file should not contain plaintext data");
    }

    [TestMethod]
    public void UnencryptedDatabase_FileContainsPlaintext()
    {
        string dbPath = Path.Combine(_testDirectory, "unencrypted.db");
        string uniqueString = "UNIQUE_SEARCHABLE_STRING_67890";
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person
            {
                Name = uniqueString,
                Age = 99,
                Email = $"{uniqueString}@example.com"
            };
            db.Insert(person);
        }

        byte[] fileContents = File.ReadAllBytes(dbPath);
        string fileAsString = Encoding.UTF8.GetString(fileContents);

        bool containsPlaintext = fileAsString.Contains(uniqueString);

        Assert.IsTrue(containsPlaintext, "Unencrypted file should contain plaintext data");
    }

    [TestMethod]
    public void EncryptedDatabase_WalFileDoesNotContainPlaintext()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        string walPath = Path.Combine(_testDirectory, "encrypted.wal");
        string uniqueString = "WAL_CONFIDENTIAL_DATA_ABCDE";
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            AutoCheckpoint = false,
            Encryption = new EncryptionOptions { Password = "walConfidentialityTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person
            {
                Name = uniqueString,
                Age = 42,
                Email = $"{uniqueString}@example.com"
            };
            db.Insert(person);
        }

        bool walContainsPlaintext = false;

        if (File.Exists(walPath))
        {
            byte[] walContents = File.ReadAllBytes(walPath);
            string walAsString = Encoding.UTF8.GetString(walContents);
            walContainsPlaintext = walAsString.Contains(uniqueString);
        }

        Assert.IsFalse(walContainsPlaintext, "Encrypted WAL file should not contain plaintext data");
    }

    #endregion

    #region Update and Delete Tests

    [TestMethod]
    public void EncryptedDatabase_UpdateDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "updateTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Original", Age = 25, Email = "original@example.com" };
            int id = db.Insert(person);

            Person toUpdate = db.GetById<Person>(id);
            toUpdate.Name = "Updated";
            toUpdate.Age = 30;
            db.Replace(toUpdate);

            Person retrieved = db.GetById<Person>(id);

            Assert.AreEqual("Updated", retrieved.Name);
            Assert.AreEqual(30, retrieved.Age);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_DeleteDocument_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "deleteTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "ToDelete", Age = 25, Email = "delete@example.com" };
            int id = db.Insert(person);

            bool deleteResult = db.DeleteById<Person>(id);
            Person retrieved = db.GetById<Person>(id);

            Assert.IsTrue(deleteResult);
            Assert.IsNull(retrieved);
        }
    }

    #endregion

    #region Transaction Tests

    [TestMethod]
    public void EncryptedDatabase_Transaction_Commit_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "transactionTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                Person person1 = new Person { Name = "TxPerson1", Age = 25, Email = "tx1@example.com" };
                Person person2 = new Person { Name = "TxPerson2", Age = 30, Email = "tx2@example.com" };

                tx.Insert(person1);
                tx.Insert(person2);

                tx.Commit();
            }

            Person retrieved1 = db.GetById<Person>(1);
            Person retrieved2 = db.GetById<Person>(2);

            Assert.IsNotNull(retrieved1);
            Assert.IsNotNull(retrieved2);
            Assert.AreEqual("TxPerson1", retrieved1.Name);
            Assert.AreEqual("TxPerson2", retrieved2.Name);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_Transaction_Rollback_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "rollbackTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person committed = new Person { Name = "Committed", Age = 25, Email = "committed@example.com" };
            db.Insert(committed);

            using (Transaction tx = db.BeginTransaction())
            {
                Person rolledBack = new Person { Name = "RolledBack", Age = 30, Email = "rolledback@example.com" };
                tx.Insert(rolledBack);

                tx.Rollback();
            }

            Person retrievedCommitted = db.GetById<Person>(1);
            Person retrievedRolledBack = db.GetById<Person>(2);

            Assert.IsNotNull(retrievedCommitted);
            Assert.AreEqual("Committed", retrievedCommitted.Name);
            Assert.IsNull(retrievedRolledBack);
        }
    }

    #endregion

    #region OpenOrCreate Tests

    [TestMethod]
    public void EncryptedDatabase_OpenOrCreate_CreatesNew_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "openOrCreateTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.OpenOrCreate(dbPath, options))
        {
            Person person = new Person { Name = "NewDb", Age = 25, Email = "new@example.com" };
            int id = db.Insert(person);

            Assert.AreEqual(1, id);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_OpenOrCreate_OpensExisting_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "openOrCreateTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "Existing", Age = 30, Email = "existing@example.com" };
            db.Insert(person);
        }

        using (GaldrDbInstance db = GaldrDbInstance.OpenOrCreate(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(1);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Existing", retrieved.Name);
        }
    }

    #endregion

    #region Async Operation Tests

    [TestMethod]
    public async Task EncryptedDatabase_InsertAsync_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "asyncInsertTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person person = new Person { Name = "AsyncPerson", Age = 35, Email = "async@example.com" };
            int id = await db.InsertAsync(person);

            Assert.AreEqual(1, id);

            Person retrieved = await db.GetByIdAsync<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("AsyncPerson", retrieved.Name);
            Assert.AreEqual(35, retrieved.Age);
        }
    }

    [TestMethod]
    public async Task EncryptedDatabase_MultipleInsertsAsync_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "asyncMultiInsert", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 50; i++)
            {
                Person person = new Person
                {
                    Name = $"AsyncPerson{i}",
                    Age = 20 + (i % 30),
                    Email = $"async{i}@example.com"
                };
                await db.InsertAsync(person);
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            for (int i = 1; i <= 50; i++)
            {
                Person retrieved = await db.GetByIdAsync<Person>(i);

                Assert.IsNotNull(retrieved, $"Person {i} should exist");
                Assert.AreEqual($"AsyncPerson{i - 1}", retrieved.Name);
            }
        }
    }

    [TestMethod]
    public async Task EncryptedDatabase_TransactionAsync_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "asyncTxTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                for (int i = 0; i < 20; i++)
                {
                    Person person = new Person
                    {
                        Name = $"TxAsyncPerson{i}",
                        Age = 25 + i,
                        Email = $"txasync{i}@example.com"
                    };
                    await tx.InsertAsync(person);
                }

                await tx.CommitAsync();
            }

            for (int i = 1; i <= 20; i++)
            {
                Person retrieved = await db.GetByIdAsync<Person>(i);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual($"TxAsyncPerson{i - 1}", retrieved.Name);
            }
        }
    }

    #endregion

    #region Secondary Index Tests

    [TestMethod]
    public void EncryptedDatabase_SecondaryIndexQuery_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "indexQueryTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Alice", Age = 35, Email = "alice2@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 28, Email = "charlie@example.com" });

            List<Person> alices = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "Alice")
                .ToList();

            Assert.HasCount(2, alices);
            Assert.IsTrue(alices.TrueForAll(p => p.Name == "Alice"));
        }
    }

    [TestMethod]
    public void EncryptedDatabase_SecondaryIndexQuery_AfterReopen_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "indexReopenTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            for (int i = 0; i < 100; i++)
            {
                string name = i % 5 == 0 ? "TargetName" : $"OtherName{i}";
                db.Insert(new Person { Name = name, Age = 20 + i, Email = $"person{i}@example.com" });
            }
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            List<Person> targets = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "TargetName")
                .ToList();

            Assert.HasCount(20, targets);
            Assert.IsTrue(targets.TrueForAll(p => p.Name == "TargetName"));
        }
    }

    [TestMethod]
    public void EncryptedDatabase_SecondaryIndexInsertAndDelete_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "indexDeleteTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            int id1 = db.Insert(new Person { Name = "ToDelete", Age = 30, Email = "delete@example.com" });
            int id2 = db.Insert(new Person { Name = "ToKeep", Age = 25, Email = "keep@example.com" });
            int id3 = db.Insert(new Person { Name = "ToDelete", Age = 35, Email = "delete2@example.com" });

            db.DeleteById<Person>(id1);

            List<Person> remaining = db.Query<Person>()
                .Where(PersonMeta.Name, FieldOp.Equals, "ToDelete")
                .ToList();

            Assert.HasCount(1, remaining);
            Assert.AreEqual(id3, remaining[0].Id);
        }
    }

    #endregion

    #region Large Document Tests

    [TestMethod]
    public void EncryptedDatabase_LargeDocument_WithWal_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "largeDocTest", KdfIterations = 1000 }
        };

        // Create a document large enough to span multiple pages
        string largeEmail = new string('x', 10000);

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            Person largePerson = new Person
            {
                Name = "LargePerson",
                Age = 40,
                Email = largeEmail
            };
            int id = db.Insert(largePerson);

            Person retrieved = db.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("LargePerson", retrieved.Name);
            Assert.AreEqual(largeEmail, retrieved.Email);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_LargeDocument_AfterReopen_Success()
    {
        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "largeDocReopenTest", KdfIterations = 1000 }
        };

        string largeEmail = new string('y', 15000);

        using (GaldrDbInstance db = GaldrDbInstance.Create(dbPath, options))
        {
            db.Insert(new Person { Name = "LargeDoc", Age = 50, Email = largeEmail });
        }

        using (GaldrDbInstance db = GaldrDbInstance.Open(dbPath, options))
        {
            Person retrieved = db.GetById<Person>(1);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("LargeDoc", retrieved.Name);
            Assert.AreEqual(largeEmail, retrieved.Email);
        }
    }

    #endregion

    #region Boundary Condition Tests

    [TestMethod]
    public void EncryptedDatabase_CollectionsMetadataNearPageBoundary_Success()
    {
        // This test verifies that collections metadata near the usable page boundary
        // survives encryption. The bug this catches: if CollectionsMetadata uses
        // full pageSize (8192) instead of usablePageSize (8160) for capacity,
        // data between 8160-8192 would be overwritten by encryption nonce/tag.

        string dbPath = Path.Combine(_testDirectory, "encrypted.db");
        int pageSize = 8192;
        int usableSize = pageSize - PageConstants.ENCRYPTION_RESERVE_SIZE; // 8160
        EncryptionOptions encOptions = new EncryptionOptions { Password = "boundaryTest", KdfIterations = 1000 };

        // We'll allocate 2 pages for collections metadata to allow spanning the boundary
        int metadataPageCount = 2;

        using (EncryptedPageIO pageIO = EncryptedPageIO.Create(dbPath, pageSize, encOptions, out _))
        {
            // Write a valid header page first (page 0)
            byte[] headerPage = new byte[pageSize];
            BinaryHelper.WriteUInt32LE(headerPage, 0, PageConstants.MAGIC_NUMBER);
            BinaryHelper.WriteInt32LE(headerPage, 4, PageConstants.VERSION);
            BinaryHelper.WriteInt32LE(headerPage, 8, pageSize);
            BinaryHelper.WriteInt32LE(headerPage, 12, 10); // TotalPageCount
            BinaryHelper.WriteInt32LE(headerPage, 16, 1);  // BitmapStartPage
            BinaryHelper.WriteInt32LE(headerPage, 20, 1);  // BitmapPageCount
            BinaryHelper.WriteInt32LE(headerPage, 24, 2);  // FsmStartPage
            BinaryHelper.WriteInt32LE(headerPage, 28, 1);  // FsmPageCount
            BinaryHelper.WriteInt32LE(headerPage, 32, 3);  // CollectionsMetadataStartPage
            BinaryHelper.WriteInt32LE(headerPage, 36, metadataPageCount);  // CollectionsMetadataPageCount
            pageIO.WritePage(0, headerPage);

            // Create CollectionsMetadata with usable page size
            CollectionsMetadata metadata = new CollectionsMetadata(
                pageIO, 3, metadataPageCount, pageSize, usableSize);

            // Add entries until we're past the first page's usable boundary
            // This forces data to span across the page boundary
            int entryCount = 0;
            while (metadata.CalculateSerializedSize() < usableSize + 500) // Go well past first page
            {
                string collectionName = $"TestCollection{entryCount:D3}WithLongNameToFillSpace";
                metadata.AddCollection(collectionName, 100 + entryCount);
                entryCount++;

                if (entryCount > 300) break; // Safety limit
            }

            int finalSize = metadata.CalculateSerializedSize();

            // Verify we've actually crossed the boundary
            Assert.IsGreaterThan(usableSize, finalSize, 
                $"Test setup error: serialized size {finalSize} should exceed usable page size {usableSize}");

            metadata.WriteToDisk();
            pageIO.Flush();

            // Reload and verify all collections survived
            CollectionsMetadata reloaded = new CollectionsMetadata(
                pageIO, 3, metadataPageCount, pageSize, usableSize);
            reloaded.LoadFromDisk();

            // Verify all collections are present
            Assert.AreEqual(entryCount, reloaded.GetCollectionCount(),
                $"Expected {entryCount} collections but found {reloaded.GetCollectionCount()}. " +
                $"Serialized size was {finalSize} bytes (usable per page: {usableSize})");

            // Verify specific collections that would span the boundary
            for (int i = 0; i < entryCount; i++)
            {
                string collectionName = $"TestCollection{i:D3}WithLongNameToFillSpace";
                CollectionEntry found = reloaded.FindCollection(collectionName);
                Assert.IsNotNull(found, $"Collection '{collectionName}' was lost - likely corrupted at page boundary");
                Assert.AreEqual(100 + i, found.RootPage, $"Collection '{collectionName}' has wrong RootPage");
            }
        }
    }

    #endregion

    #region CompactTo Tests

    [TestMethod]
    public void EncryptedDatabase_CompactTo_CopiesAllDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "compactTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            for (int i = 0; i < 10; i++)
            {
                db.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(1, result.CollectionsCompacted);
            Assert.AreEqual(10, result.DocumentsCopied);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            List<Person> people = targetDb.Query<Person>().ToList();

            Assert.HasCount(10, people);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_TargetRequiresSamePassword()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "compactPasswordTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
            db.CompactTo(targetPath);
        }

        GaldrDbOptions wrongOptions = new GaldrDbOptions
        {
            Encryption = new EncryptionOptions { Password = "wrongPassword", KdfIterations = 1000 }
        };

        Assert.Throws<InvalidPasswordException>(() =>
        {
            using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, wrongOptions))
            {
            }
        });
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_TargetFileIsEncrypted()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "encryptedTargetTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = "TestPerson", Age = 25, Email = "test@example.com" });
            db.CompactTo(targetPath);
        }

        bool isEncrypted = EncryptedPageIO.IsEncryptedFile(targetPath);

        Assert.IsTrue(isEncrypted, "Compacted target file should be encrypted");
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_TargetDoesNotContainPlaintext()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        string uniqueString = "COMPACT_UNIQUE_STRING_98765";
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "plaintextTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = uniqueString, Age = 30, Email = $"{uniqueString}@example.com" });
            db.CompactTo(targetPath);
        }

        byte[] targetContents = File.ReadAllBytes(targetPath);
        string targetAsString = Encoding.UTF8.GetString(targetContents);

        bool containsPlaintext = targetAsString.Contains(uniqueString);

        Assert.IsFalse(containsPlaintext, "Compacted target file should not contain plaintext data");
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_PreservesDocumentIds()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "preserveIdsTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });
            db.CompactTo(targetPath);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            Person alice = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 1).FirstOrDefault();
            Person bob = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 2).FirstOrDefault();
            Person charlie = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 3).FirstOrDefault();

            Assert.IsNotNull(alice);
            Assert.AreEqual("Alice", alice.Name);
            Assert.IsNotNull(bob);
            Assert.AreEqual("Bob", bob.Name);
            Assert.IsNotNull(charlie);
            Assert.AreEqual("Charlie", charlie.Name);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_ExcludesDeletedDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "deletedExcludeTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = "Alice", Age = 30, Email = "alice@example.com" });
            db.Insert(new Person { Name = "Bob", Age = 25, Email = "bob@example.com" });
            db.Insert(new Person { Name = "Charlie", Age = 35, Email = "charlie@example.com" });

            db.DeleteById<Person>(2);

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(2, result.DocumentsCopied);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            List<Person> people = targetDb.Query<Person>().ToList();

            Assert.HasCount(2, people);

            Person bob = targetDb.Query<Person>().Where(PersonMeta.Id, FieldOp.Equals, 2).FirstOrDefault();

            Assert.IsNull(bob);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_WithoutWal_Success()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "noWalCompactTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            for (int i = 0; i < 20; i++)
            {
                db.Insert(new Person { Name = $"Person{i}", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(20, result.DocumentsCopied);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            List<Person> people = targetDb.Query<Person>().ToList();

            Assert.HasCount(20, people);

            for (int i = 0; i < 20; i++)
            {
                Person person = targetDb.GetById<Person>(i + 1);

                Assert.IsNotNull(person);
                Assert.AreEqual($"Person{i}", person.Name);
            }
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_PreservesSecondaryIndex()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "indexCompactTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new User { Name = "Alice", Email = "alice@example.com", Department = "Engineering" });
            db.Insert(new User { Name = "Bob", Email = "bob@example.com", Department = "Engineering" });
            db.Insert(new User { Name = "Charlie", Email = "charlie@example.com", Department = "Sales" });

            db.CompactTo(targetPath);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            User bob = targetDb.Query<User>()
                .Where(UserMeta.Email, FieldOp.Equals, "bob@example.com")
                .FirstOrDefault();

            Assert.IsNotNull(bob);
            Assert.AreEqual("Bob", bob.Name);
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_ReducesFileSizeAfterDeletes()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            ExpansionPageCount = 32,
            Encryption = new EncryptionOptions { Password = "sizeReductionTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            // Insert enough documents to cause multiple expansions
            for (int i = 0; i < 2000; i++)
            {
                db.Insert(new Person { Name = $"Person{i} with a longer name to take up more space in the database file", Age = 20 + i, Email = $"person{i}@example.com" });
            }

            // Delete most documents
            for (int i = 1; i <= 1900; i++)
            {
                db.DeleteById<Person>(i);
            }

            long sourceSize = new FileInfo(sourcePath).Length;
            DatabaseCompactResult result = db.CompactTo(targetPath);

            Assert.AreEqual(100, result.DocumentsCopied);
            Assert.IsLessThan(sourceSize, result.TargetFileSize, $"Target size {result.TargetFileSize} should be less than source size {sourceSize}");
            Assert.IsGreaterThan(0, result.BytesSaved);
        }
    }

    [TestMethod]
    public async Task EncryptedDatabase_CompactToAsync_CopiesAllDocuments()
    {
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            Encryption = new EncryptionOptions { Password = "asyncCompactTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            for (int i = 0; i < 15; i++)
            {
                await db.InsertAsync(new Person { Name = $"AsyncPerson{i}", Age = 25 + i, Email = $"async{i}@example.com" });
            }

            DatabaseCompactResult result = await db.CompactToAsync(targetPath);

            Assert.AreEqual(1, result.CollectionsCompacted);
            Assert.AreEqual(15, result.DocumentsCopied);
        }

        using (GaldrDbInstance targetDb = GaldrDbInstance.Open(targetPath, options))
        {
            List<Person> people = targetDb.Query<Person>().ToList();

            Assert.HasCount(15, people);

            for (int i = 0; i < 15; i++)
            {
                Person person = await targetDb.GetByIdAsync<Person>(i + 1);

                Assert.IsNotNull(person);
                Assert.AreEqual($"AsyncPerson{i}", person.Name);
            }
        }
    }

    [TestMethod]
    public void EncryptedDatabase_CompactTo_TargetHasDifferentSalt()
    {
        // Verify the target database uses fresh cryptographic material (different salt)
        // by checking that the encrypted bytes at the same page position are different
        string sourcePath = Path.Combine(_testDirectory, "source.db");
        string targetPath = Path.Combine(_testDirectory, "target.db");
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = false,
            Encryption = new EncryptionOptions { Password = "saltTest", KdfIterations = 1000 }
        };

        using (GaldrDbInstance db = GaldrDbInstance.Create(sourcePath, options))
        {
            db.Insert(new Person { Name = "SaltTestPerson", Age = 30, Email = "salt@example.com" });
            db.CompactTo(targetPath);
        }

        // Read the encryption headers from both files
        byte[] sourceHeader = new byte[32];
        byte[] targetHeader = new byte[32];

        using (FileStream fs = new FileStream(sourcePath, FileMode.Open, FileAccess.Read))
        {
            fs.ReadExactly(sourceHeader, 0, 32);
        }

        using (FileStream fs = new FileStream(targetPath, FileMode.Open, FileAccess.Read))
        {
            fs.ReadExactly(targetHeader, 0, 32);
        }

        // Salt is at offset 12, 16 bytes long
        byte[] sourceSalt = new byte[16];
        byte[] targetSalt = new byte[16];
        Array.Copy(sourceHeader, 12, sourceSalt, 0, 16);
        Array.Copy(targetHeader, 12, targetSalt, 0, 16);

        bool saltsAreDifferent = false;
        for (int i = 0; i < 16; i++)
        {
            if (sourceSalt[i] != targetSalt[i])
            {
                saltsAreDifferent = true;
                break;
            }
        }

        Assert.IsTrue(saltsAreDifferent, "Target database should have a different salt than source");
    }

    #endregion
}
