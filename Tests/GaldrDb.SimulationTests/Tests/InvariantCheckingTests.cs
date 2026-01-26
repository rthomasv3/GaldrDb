using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Simulation;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class InvariantCheckingTests
{
    [TestMethod]
    public void InvariantChecker_AllInvariantsPass_WhenStateMatches()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            // Insert some documents and track state
            for (int i = 0; i < 5; i++)
            {
                using (Transaction tx = db.BeginTransaction())
                {
                    TestDocument doc = TestDocument.Generate(rng, 200);
                    int docId = tx.Insert(doc);
                    byte[] hash = doc.ComputeHash();
                    tx.Commit();

                    state.RecordInsert("TestDocument", docId, hash, tx.TxId.Value);
                }
            }

            // Check invariants - should all pass
            InvariantCheckResult result = checker.CheckAll(db, state);

            Assert.IsTrue(result.AllPassed, $"All invariants should pass. Violations: {string.Join(", ", result.Violations)}");
            Assert.AreEqual(0, result.ViolationsFound);
            Assert.IsGreaterThan(0, result.ChecksPerformed);
        }
    }

    [TestMethod]
    public void InvariantChecker_DetectsMissingDocument()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test2.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            // Insert a document
            using (Transaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                int docId = tx.Insert(doc);
                byte[] hash = doc.ComputeHash();
                tx.Commit();

                state.RecordInsert("TestDocument", docId, hash, tx.TxId.Value);
            }

            // Add a fake document to state that doesn't exist in DB
            state.RecordInsert("TestDocument", 9999, new byte[] { 1, 2, 3 }, 999);

            // Check invariants - should detect missing document
            InvariantCheckResult result = checker.CheckAll(db, state);

            Assert.IsFalse(result.AllPassed, "Should detect missing document");
            Assert.IsGreaterThan(0, result.ViolationsFound);

            bool foundMissingDocViolation = false;
            foreach (InvariantViolation v in result.Violations)
            {
                if (v.Type == InvariantType.DocumentExists && v.DocumentId == 9999)
                {
                    foundMissingDocViolation = true;
                    break;
                }
            }
            Assert.IsTrue(foundMissingDocViolation, "Should have violation for missing document 9999");
        }
    }

    [TestMethod]
    public void InvariantChecker_DetectsContentMismatch()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test3.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            int insertedId;
            using (Transaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                insertedId = tx.Insert(doc);
                tx.Commit();

                // Record with WRONG hash
                state.RecordInsert("TestDocument", insertedId, new byte[] { 0xFF, 0xFF, 0xFF }, tx.TxId.Value);
            }

            // Check invariants - should detect content mismatch
            InvariantCheckResult result = checker.CheckAll(db, state);

            Assert.IsFalse(result.AllPassed, "Should detect content mismatch");

            bool foundContentViolation = false;
            foreach (InvariantViolation v in result.Violations)
            {
                if (v.Type == InvariantType.DocumentContent && v.DocumentId == insertedId)
                {
                    foundContentViolation = true;
                    break;
                }
            }
            Assert.IsTrue(foundContentViolation, "Should have violation for content mismatch");
        }
    }

    [TestMethod]
    public void InvariantChecker_QuickCheck_ReturnsTrueWhenValid()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test4.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            // Insert documents
            for (int i = 0; i < 3; i++)
            {
                using (Transaction tx = db.BeginTransaction())
                {
                    TestDocument doc = TestDocument.Generate(rng, 200);
                    int docId = tx.Insert(doc);
                    byte[] hash = doc.ComputeHash();
                    tx.Commit();

                    state.RecordInsert("TestDocument", docId, hash, tx.TxId.Value);
                }
            }

            // Quick check should pass
            bool quickResult = checker.QuickCheck(db, state);
            Assert.IsTrue(quickResult, "Quick check should pass when state matches");
        }
    }

    [TestMethod]
    public void InvariantChecker_QuickCheck_ReturnsFalseWhenInvalid()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test5.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            // Insert one document
            using (Transaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                int docId = tx.Insert(doc);
                byte[] hash = doc.ComputeHash();
                tx.Commit();

                state.RecordInsert("TestDocument", docId, hash, tx.TxId.Value);
            }

            // Add fake document to state
            state.RecordInsert("TestDocument", 8888, new byte[] { 1 }, 888);

            // Quick check should fail
            bool quickResult = checker.QuickCheck(db, state);
            Assert.IsFalse(quickResult, "Quick check should fail when document missing");
        }
    }

    [TestMethod]
    public void InvariantChecker_ConsistentReads_PassesForValidData()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);
        SimulationState state = new SimulationState();
        InvariantChecker checker = new InvariantChecker(stats);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("invariant_test6.db", options))
        {
            pageIO.Flush();
            state.EnsureCollection("TestDocument");

            // Insert a document
            using (Transaction tx = db.BeginTransaction())
            {
                TestDocument doc = TestDocument.Generate(rng, 200);
                int docId = tx.Insert(doc);
                byte[] hash = doc.ComputeHash();
                tx.Commit();

                state.RecordInsert("TestDocument", docId, hash, tx.TxId.Value);
            }

            // Check consistent reads specifically
            InvariantCheckResult result = new InvariantCheckResult();
            checker.CheckConsistentReads(db, state, result);

            Assert.IsTrue(result.AllPassed, "Consistent reads should pass for valid data");
        }
    }

    [TestMethod]
    public void SimulationRunner_WithInvariantChecker_MaintainsInvariants()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 50;
        config.Seed = 12345;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Run operations
        runner.Run();

        // Verify invariants hold
        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid after operations");

        runner.Shutdown();
    }

    [TestMethod]
    public void InvariantChecker_AfterCrashRecovery_InvariantsHold()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 30;
        config.Seed = 54321;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Run some operations
        runner.Run();

        // Crash and recover
        runner.SimulateCrash();
        runner.Recover();

        // Verify invariants still hold after recovery
        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "Invariants should hold after crash recovery");

        runner.Shutdown();
    }
}
