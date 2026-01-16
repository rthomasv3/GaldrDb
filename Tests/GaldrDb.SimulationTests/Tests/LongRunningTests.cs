using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Simulation;
using GaldrDb.SimulationTests.Workload;

namespace GaldrDb.SimulationTests.Tests;

/// <summary>
/// Long-running simulation tests for stress testing the database.
/// These tests run many operations with crashes, faults, and invariant checks.
/// </summary>
[TestClass]
public class LongRunningTests
{
    [TestMethod]
    public void StressTest_1000Operations_NoFaults()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 1000;
        config.Seed = 98765;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.AreEqual(1000, result.TotalOperations);
        Assert.IsGreaterThan(0, result.SuccessCount);

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid after 1000 operations");

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_WriteHeavy_500Operations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 500;
        config.Seed = 11111;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.IsGreaterThan(0, runner.State.GetTotalDocumentCount());

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid after write-heavy workload");

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_HighChurn_500Operations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.HighChurn();
        config.OperationCount = 500;
        config.Seed = 22222;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        runner.Run();

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid after high-churn workload");

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_WithPeriodicCrashes_500Operations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 100;
        config.Seed = 33333;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Run 5 batches with crash/recovery between each
        for (int batch = 0; batch < 5; batch++)
        {
            runner.Run();

            // Verify state before crash
            bool preValid = runner.VerifyState();
            Assert.IsTrue(preValid, $"State should be valid before crash {batch + 1}");

            runner.SimulateCrash();
            runner.Recover();

            // Verify state after recovery
            bool postValid = runner.VerifyState();
            Assert.IsTrue(postValid, $"State should be valid after crash {batch + 1}");
        }

        Assert.AreEqual(5, runner.Stats.CrashCount);
        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_RapidCrashRecovery_20Cycles()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 20;
        config.Seed = 44444;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        // Rapid crash/recovery cycles with few operations each
        for (int cycle = 0; cycle < 20; cycle++)
        {
            runner.Run();
            runner.SimulateCrash();
            runner.Recover();
        }

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid after 20 crash cycles");
        Assert.AreEqual(20, runner.Stats.CrashCount);

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_MixedWorkloads_Sequential()
    {
        int seed = 55555;
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationRandom rng = new SimulationRandom(seed);

        // Run different workload types sequentially on the same database
        WorkloadConfiguration[] configs = new WorkloadConfiguration[]
        {
            WorkloadConfiguration.WriteHeavy(),
            WorkloadConfiguration.Balanced(),
            WorkloadConfiguration.HighChurn(),
            WorkloadConfiguration.ReadHeavy()
        };

        SimulationState sharedState = new SimulationState();
        sharedState.EnsureCollection("TestDocument");

        GaldrDbEngine.GaldrDbOptions options = new GaldrDbEngine.GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStream = walStream,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        using (GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("stress_mixed.db", options))
        {
            pageIO.Flush();

            foreach (WorkloadConfiguration config in configs)
            {
                config.OperationCount = 100;
                config.Seed = seed++;

                WorkloadGenerator generator = new WorkloadGenerator(config, new SimulationRandom(config.Seed));

                for (int i = 0; i < config.OperationCount; i++)
                {
                    GaldrDb.SimulationTests.Workload.Operations.Operation op = generator.GenerateOperation(sharedState);

                    using (GaldrDbEngine.Transactions.Transaction tx = db.BeginTransaction())
                    {
                        GaldrDb.SimulationTests.Workload.Operations.OperationResult result = op.Execute(db, tx, sharedState);

                        if (result.Success)
                        {
                            tx.Commit();

                            if (op is GaldrDb.SimulationTests.Workload.Operations.InsertOperation && result.DocId.HasValue)
                            {
                                sharedState.RecordInsert(op.CollectionName, result.DocId.Value, result.ContentHash, tx.TxId.Value);
                            }
                            else if (op is GaldrDb.SimulationTests.Workload.Operations.UpdateOperation updateOp && result.DocId.HasValue)
                            {
                                sharedState.RecordUpdate(op.CollectionName, updateOp.DocId, result.ContentHash, tx.TxId.Value);
                            }
                            else if (op is GaldrDb.SimulationTests.Workload.Operations.DeleteOperation deleteOp)
                            {
                                sharedState.RecordDelete(op.CollectionName, deleteOp.DocId);
                            }
                        }
                    }
                }
            }

            // Verify final state
            InvariantChecker checker = new InvariantChecker(stats);
            InvariantCheckResult checkResult = checker.CheckAll(db, sharedState);

            Assert.IsTrue(checkResult.AllPassed, $"Invariants should hold after mixed workloads. Violations: {string.Join(", ", checkResult.Violations)}");
        }
    }

    [TestMethod]
    public void StressTest_LargeDocuments_100Operations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 100;
        config.Seed = 66666;
        config.MinPayloadSize = 1000;
        config.MaxPayloadSize = 5000;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        runner.Run();

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid with large documents");

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_SmallDocuments_1000Operations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 1000;
        config.Seed = 77777;
        config.MinPayloadSize = 10;
        config.MaxPayloadSize = 50;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        runner.Run();

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "State should be valid with small documents");

        Assert.IsGreaterThan(100, runner.State.GetTotalDocumentCount(), "Should have many documents");

        runner.Shutdown();
    }

    [TestMethod]
    public void StressTest_Deterministic_SameSeedSameResult()
    {
        int seed = 88888;

        // Run simulation twice with same seed
        int count1;
        int count2;

        {
            WorkloadConfiguration config = WorkloadConfiguration.Balanced();
            config.OperationCount = 200;
            config.Seed = seed;

            SimulationRunner runner = new SimulationRunner(config, config.Seed);
            runner.Initialize();
            runner.Run();
            count1 = runner.State.GetTotalDocumentCount();
            runner.Shutdown();
        }

        {
            WorkloadConfiguration config = WorkloadConfiguration.Balanced();
            config.OperationCount = 200;
            config.Seed = seed;

            SimulationRunner runner = new SimulationRunner(config, config.Seed);
            runner.Initialize();
            runner.Run();
            count2 = runner.State.GetTotalDocumentCount();
            runner.Shutdown();
        }

        Assert.AreEqual(count1, count2, "Same seed should produce same document count");
    }

    [TestMethod]
    public void StressTest_CrashAtRandomPoints_10Runs()
    {
        SimulationRandom seedRng = new SimulationRandom(99999);

        for (int run = 0; run < 10; run++)
        {
            int seed = seedRng.Next();

            WorkloadConfiguration config = WorkloadConfiguration.Balanced();
            config.OperationCount = 50;
            config.Seed = seed;

            SimulationRunner runner = new SimulationRunner(config, config.Seed);
            runner.Initialize();

            // Run some operations
            runner.Run();

            // Random crash point
            runner.SimulateCrash();
            runner.Recover();

            // Run more operations
            config.OperationCount = 30;
            runner.Run();

            // Verify
            bool stateValid = runner.VerifyState();
            Assert.IsTrue(stateValid, $"Run {run + 1} with seed {seed} should have valid state");

            runner.Shutdown();
        }
    }

    [TestMethod]
    [Timeout(30000, CooperativeCancellation = true)] // 30 second timeout
    public void EnduranceTest_ContinuousOperations()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 100;
        config.Seed = 100100;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        int totalOperations = 0;
        int crashCount = 0;
        DateTime startTime = DateTime.UtcNow;
        TimeSpan maxDuration = TimeSpan.FromSeconds(10);

        while (DateTime.UtcNow - startTime < maxDuration)
        {
            runner.Run();
            totalOperations += config.OperationCount;

            // Occasional crash
            if (totalOperations % 300 == 0)
            {
                runner.SimulateCrash();
                runner.Recover();
                crashCount++;
            }

            // Periodic invariant check
            if (totalOperations % 500 == 0)
            {
                bool valid = runner.VerifyState();
                Assert.IsTrue(valid, $"State invalid after {totalOperations} operations");
            }
        }

        // Final verification
        bool finalValid = runner.VerifyState();
        Assert.IsTrue(finalValid, "Final state should be valid");

        Console.WriteLine($"Endurance test completed: {totalOperations} operations, {crashCount} crashes");

        runner.Shutdown();
    }
}
