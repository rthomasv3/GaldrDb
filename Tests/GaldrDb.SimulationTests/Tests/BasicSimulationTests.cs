using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GaldrDb.SimulationTests.Simulation;
using GaldrDb.SimulationTests.Workload;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class BasicSimulationTests
{
    [TestMethod]
    public void Simulation_1000Operations_Balanced_StateConsistent()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 1000;
        config.Seed = 42;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.AreEqual(1000, result.TotalOperations);
        Assert.IsGreaterThan(0, result.SuccessCount, "Should have some successful operations");
        Assert.IsGreaterThan(0, result.FinalDocumentCount, "Should have documents after simulation");

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "Database state should match expected state");

        runner.Shutdown();
    }

    [TestMethod]
    public void Simulation_1000Operations_WriteHeavy_StateConsistent()
    {
        WorkloadConfiguration config = WorkloadConfiguration.WriteHeavy();
        config.OperationCount = 1000;
        config.Seed = 123;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.AreEqual(1000, result.TotalOperations);
        Assert.IsGreaterThan(0, result.SuccessCount);
        Assert.IsGreaterThan(0, result.FinalDocumentCount);

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "Database state should match expected state");

        runner.Shutdown();
    }

    [TestMethod]
    public void Simulation_1000Operations_HighChurn_StateConsistent()
    {
        WorkloadConfiguration config = WorkloadConfiguration.HighChurn();
        config.OperationCount = 1000;
        config.Seed = 456;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.AreEqual(1000, result.TotalOperations);
        Assert.IsGreaterThan(0, result.SuccessCount);

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "Database state should match expected state");

        runner.Shutdown();
    }

    [TestMethod]
    public void Simulation_Deterministic_SameSeedProducesSameResult()
    {
        WorkloadConfiguration config1 = WorkloadConfiguration.Balanced();
        config1.OperationCount = 500;
        config1.Seed = 999;

        WorkloadConfiguration config2 = WorkloadConfiguration.Balanced();
        config2.OperationCount = 500;
        config2.Seed = 999;

        SimulationRunner runner1 = new SimulationRunner(config1, config1.Seed);
        runner1.Initialize();
        SimulationResult result1 = runner1.Run();
        int docCount1 = runner1.State.GetTotalDocumentCount();
        runner1.Shutdown();

        SimulationRunner runner2 = new SimulationRunner(config2, config2.Seed);
        runner2.Initialize();
        SimulationResult result2 = runner2.Run();
        int docCount2 = runner2.State.GetTotalDocumentCount();
        runner2.Shutdown();

        Assert.AreEqual(result1.SuccessCount, result2.SuccessCount, "Same seed should produce same success count");
        Assert.AreEqual(result1.FailureCount, result2.FailureCount, "Same seed should produce same failure count");
        Assert.AreEqual(docCount1, docCount2, "Same seed should produce same document count");
    }

    [TestMethod]
    public void Simulation_10000Operations_Balanced_StateConsistent()
    {
        WorkloadConfiguration config = WorkloadConfiguration.Balanced();
        config.OperationCount = 10000;
        config.Seed = 12345;

        SimulationRunner runner = new SimulationRunner(config, config.Seed);
        runner.Initialize();

        SimulationResult result = runner.Run();

        Assert.AreEqual(10000, result.TotalOperations);
        Assert.IsGreaterThan(0, result.SuccessCount);
        Assert.IsGreaterThan(0, result.FinalDocumentCount);

        bool stateValid = runner.VerifyState();
        Assert.IsTrue(stateValid, "Database state should match expected state");

        Console.WriteLine($"Duration: {result.Duration.TotalMilliseconds:F2}ms");
        Console.WriteLine($"Operations/sec: {result.OperationsPerSecond:F0}");
        Console.WriteLine($"Final docs: {result.FinalDocumentCount}");
        Console.WriteLine($"Stats: {runner.Stats}");

        runner.Shutdown();
    }
}
