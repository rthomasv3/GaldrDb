using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Simulation;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class CrashInjectionTests
{
    [TestMethod]
    public void CrashAfterCommit_DataSurvives()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        // Create database and insert a document
        int insertedId;
        byte[] originalHash;
        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("crash_test.db", options))
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                insertedId = tx.Insert(doc);
                originalHash = doc.ComputeHash();
                tx.Commit();
            }
        }

        // Simulate crash (discard unflushed data)
        pageIO.SimulateCrash();
        walStream.SimulateCrash();

        // Recover and verify data
        options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Open("crash_test.db", options))
        {
            using (ITransaction tx = db.BeginReadOnlyTransaction())
            {
                TestDocument recovered = tx.GetById<TestDocument>(insertedId);
                Assert.IsNotNull(recovered, "Document should survive crash after commit");

                byte[] recoveredHash = recovered.ComputeHash();
                CollectionAssert.AreEqual(originalHash, recoveredHash, "Document content should match after recovery");
            }
        }
    }

    [TestMethod]
    public void CrashBeforeCommit_DataLost()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        // Create database
        GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("crash_test2.db", options);

        // Flush initial setup to ensure collections metadata is durable
        pageIO.Flush();

        // First, commit some data to establish a baseline
        int committedId;
        using (ITransaction tx = db.BeginTransaction())
        {
            TestDocument doc = TestDocument.Generate(rng, 200);
            doc.Name = "Committed";
            committedId = tx.Insert(doc);
            tx.Commit();
        }

        // Start a transaction but don't commit
        ITransaction uncommittedTx = db.BeginTransaction();
        TestDocument uncommittedDoc = TestDocument.Generate(rng, 200);
        uncommittedDoc.Name = "Uncommitted";
        int uncommittedId = uncommittedTx.Insert(uncommittedDoc);
        // Do NOT commit - simulate crash mid-transaction

        // Abandon database and simulate crash
        db = null;
        pageIO.SimulateCrash();
        walStream.SimulateCrash();

        // Recover
        options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db2 = GaldrDbEngine.GaldrDb.Open("crash_test2.db", options))
        {
            using (ITransaction tx = db2.BeginReadOnlyTransaction())
            {
                // Committed document should exist
                TestDocument committed = tx.GetById<TestDocument>(committedId);
                Assert.IsNotNull(committed, "Committed document should survive crash");
                Assert.AreEqual("Committed", committed.Name);

                // Uncommitted document should NOT exist
                TestDocument uncommitted = tx.GetById<TestDocument>(uncommittedId);
                Assert.IsNull(uncommitted, "Uncommitted document should be lost after crash");
            }
        }
    }

    [TestMethod]
    public void MultipleOperationsThenCrash_OnlyCommittedSurvive()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 100;
        config.Seed = 123;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Run some operations
        SimulationResult result1 = runner.Run();
        int docCountBeforeCrash = runner.State.GetTotalDocumentCount();

        Assert.IsGreaterThan(0, docCountBeforeCrash, "Should have documents before crash");

        // Simulate crash and recover
        runner.SimulateCrash();
        runner.Recover();

        // Verify state
        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be consistent after crash recovery");

        runner.Shutdown();
    }

    [TestMethod]
    public void CrashDuringWorkload_RecoverAndContinue()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 50;
        config.Seed = 456;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Run first batch
        runner.Run();
        int countAfterBatch1 = runner.State.GetTotalDocumentCount();

        // Crash and recover
        runner.SimulateCrash();
        runner.Recover();

        // Verify first batch survived
        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be consistent after first crash");

        // Run second batch
        config.OperationCount = 50;
        runner.Run();

        // Crash and recover again
        runner.SimulateCrash();
        runner.Recover();

        // Verify all data survived
        stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be consistent after second crash");

        runner.Shutdown();
    }

    [TestMethod]
    public void RepeatedCrashes_DatabaseRemainsConsistent()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 20;
        config.Seed = 789;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Crash and recover multiple times with operations in between
        for (int i = 0; i < 5; i++)
        {
            runner.Run();
            runner.SimulateCrash();
            runner.Recover();

            bool stateValid = runner.VerifyState();
            Assert.IsTrue(stateValid, $"State should be consistent after crash {i + 1}");
        }

        Assert.IsGreaterThan(0, runner.Stats.CrashCount, "Should have recorded crashes");
        runner.Shutdown();
    }

    [TestMethod]
    public void CrashWithHighChurn_DeletedItemsStayDeleted()
    {
        WorkloadConfiguration config = WorkloadConfiguration.HighChurn();
        config.OperationCount = 100;
        config.Seed = 999;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        runner.Run();

        // Record document count before crash
        int countBeforeCrash = runner.State.GetTotalDocumentCount();

        runner.SimulateCrash();
        runner.Recover();

        // Verify state - deleted items should stay deleted
        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be consistent with high churn after crash");

        // Document count should match expected (some may have been deleted)
        int countAfterRecovery = runner.State.GetTotalDocumentCount();
        Assert.AreEqual(countBeforeCrash, countAfterRecovery, "Document count should match after recovery");

        runner.Shutdown();
    }
}
