using System;
using System.Collections.Generic;
using System.IO;
using GaldrDb.UnitTests.TestModels;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using GaldrDatabase = GaldrDbEngine.GaldrDb;

namespace GaldrDb.UnitTests;

[TestClass]
public class SnapshotIsolationTests
{
    private string _testDbPath;

    [TestInitialize]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"snapshot_test_{Guid.NewGuid()}.gdb");
    }

    [TestCleanup]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }

        string walPath = Path.ChangeExtension(_testDbPath, ".wal");
        if (File.Exists(walPath))
        {
            File.Delete(walPath);
        }
    }

    [TestMethod]
    public void Transaction_GetById_SeesOwnInsert()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                Person person = new Person { Name = "Alice", Age = 30 };
                int id = tx.Insert(person);

                Person retrieved = tx.GetById<Person>(id);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Alice", retrieved.Name);
                Assert.AreEqual(30, retrieved.Age);

                tx.Commit();
            }
        }
    }

    [TestMethod]
    public void Transaction_GetById_SeesOwnUpdate()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Bob", Age = 25 };
            int id = db.Insert(person);

            using (Transaction tx = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Bob Updated", Age = 26 };
                tx.Update(updated);

                Person retrieved = tx.GetById<Person>(id);

                Assert.IsNotNull(retrieved);
                Assert.AreEqual("Bob Updated", retrieved.Name);
                Assert.AreEqual(26, retrieved.Age);

                tx.Commit();
            }
        }
    }

    [TestMethod]
    public void Transaction_GetById_DoesNotSeeOwnDelete()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Charlie", Age = 35 };
            int id = db.Insert(person);

            using (Transaction tx = db.BeginTransaction())
            {
                tx.Delete<Person>(id);

                Person retrieved = tx.GetById<Person>(id);

                Assert.IsNull(retrieved);

                tx.Commit();
            }
        }
    }

    [TestMethod]
    public void Transaction_GetById_DoesNotSeeConcurrentInsert()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Transaction tx1 = db.BeginTransaction();

            int id;
            using (Transaction tx2 = db.BeginTransaction())
            {
                Person person = new Person { Name = "Diana", Age = 40 };
                id = tx2.Insert(person);
                tx2.Commit();
            }

            Person retrieved = tx1.GetById<Person>(id);

            Assert.IsNull(retrieved);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_GetById_DoesNotSeeConcurrentUpdate()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Eve", Age = 28 };
            int id = db.Insert(person);

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Eve Updated", Age = 29 };
                tx2.Update(updated);
                tx2.Commit();
            }

            Person retrieved = tx1.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Eve", retrieved.Name);
            Assert.AreEqual(28, retrieved.Age);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_GetById_DoesNotSeeConcurrentDelete()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Frank", Age = 50 };
            int id = db.Insert(person);

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                tx2.Delete<Person>(id);
                tx2.Commit();
            }

            Person retrieved = tx1.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Frank", retrieved.Name);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Query_SeesOwnInserts()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.Insert(new Person { Name = "Grace", Age = 30 });
                tx.Insert(new Person { Name = "Henry", Age = 35 });

                List<Person> results = tx.Query<Person>().ToList();

                Assert.HasCount(2, results);

                tx.Commit();
            }
        }
    }

    [TestMethod]
    public void Transaction_Query_DoesNotSeeConcurrentInserts()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                tx2.Insert(new Person { Name = "Ivan", Age = 45 });
                tx2.Insert(new Person { Name = "Julia", Age = 33 });
                tx2.Commit();
            }

            List<Person> results = tx1.Query<Person>().ToList();

            Assert.IsEmpty(results);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Query_SeesSnapshotVersion()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Kate", Age = 25 };
            int id = db.Insert(person);

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Kate Updated", Age = 26 };
                tx2.Update(updated);
                tx2.Commit();
            }

            List<Person> results = tx1.Query<Person>().ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Kate", results[0].Name);
            Assert.AreEqual(25, results[0].Age);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Query_DoesNotSeeConcurrentDeletes()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Leo", Age = 40 };
            int id = db.Insert(person);

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                tx2.Delete<Person>(id);
                tx2.Commit();
            }

            List<Person> results = tx1.Query<Person>().ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Leo", results[0].Name);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_Query_WithFilter_RespectsSnapshot()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            db.Insert(new Person { Name = "Mary", Age = 25 });
            db.Insert(new Person { Name = "Nancy", Age = 35 });

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                tx2.Insert(new Person { Name = "Oscar", Age = 30 });
                tx2.Commit();
            }

            List<Person> results = tx1.Query<Person>()
                .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 30)
                .ToList();

            Assert.HasCount(1, results);
            Assert.AreEqual("Nancy", results[0].Name);

            tx1.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_ReadOnlyTransaction_SeesSnapshot()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Paul", Age = 45 };
            int id = db.Insert(person);

            Transaction readTx = db.BeginReadOnlyTransaction();

            using (Transaction writeTx = db.BeginTransaction())
            {
                Person updated = new Person { Id = id, Name = "Paul Updated", Age = 46 };
                writeTx.Update(updated);
                writeTx.Commit();
            }

            Person retrieved = readTx.GetById<Person>(id);

            Assert.IsNotNull(retrieved);
            Assert.AreEqual("Paul", retrieved.Name);
            Assert.AreEqual(45, retrieved.Age);

            readTx.Dispose();
        }
    }

    [TestMethod]
    public void Transaction_MultipleVersions_SeesCorrectVersion()
    {
        using (GaldrDatabase db = GaldrDatabase.Create(_testDbPath, new GaldrDbOptions()))
        {
            Person person = new Person { Name = "Quinn v1", Age = 20 };
            int id = db.Insert(person);

            Transaction tx1 = db.BeginTransaction();

            using (Transaction tx2 = db.BeginTransaction())
            {
                Person v2 = new Person { Id = id, Name = "Quinn v2", Age = 21 };
                tx2.Update(v2);
                tx2.Commit();
            }

            Transaction tx3 = db.BeginTransaction();

            using (Transaction tx4 = db.BeginTransaction())
            {
                Person v3 = new Person { Id = id, Name = "Quinn v3", Age = 22 };
                tx4.Update(v3);
                tx4.Commit();
            }

            Person fromTx1 = tx1.GetById<Person>(id);
            Person fromTx3 = tx3.GetById<Person>(id);
            Person current = db.GetById<Person>(id);

            Assert.AreEqual("Quinn v1", fromTx1.Name);
            Assert.AreEqual("Quinn v2", fromTx3.Name);
            Assert.AreEqual("Quinn v3", current.Name);

            tx1.Dispose();
            tx3.Dispose();
        }
    }
}
