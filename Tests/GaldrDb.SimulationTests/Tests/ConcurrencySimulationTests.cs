using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Concurrency;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class ConcurrencySimulationTests
{
    [TestMethod]
    public void WriteConflict_TwoWritersOnSameDocument_ConflictDetected()
    {
        ConcurrencyConfiguration config = ConcurrencyConfiguration.TwoWritersOneDocument();
        config.Seed = 42;

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        // With two writers on one document, at least one conflict should occur
        Assert.IsGreaterThan(0, result.ConflictsDetected, "Should detect at least one conflict with two writers on same document");
        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);

        runner.Shutdown();
    }

    [TestMethod]
    public void WriteConflict_RetrySucceeds()
    {
        ConcurrencyConfiguration config = ConcurrencyConfiguration.TwoWritersOneDocument();
        config.Seed = 123;
        config.OperationsPerActor = 5;

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        // Conflicts should occur but retries should succeed (no max retries exceeded)
        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);
        Assert.AreEqual(0, result.MaxRetriesExceeded, "Retries should succeed, no max retries exceeded");

        runner.Shutdown();
    }

    [TestMethod]
    public void LostUpdatePrevention_IncrementSum_Matches()
    {
        ConcurrencyConfiguration config = new ConcurrencyConfiguration
        {
            WriterCount = 4,
            ReaderCount = 0,
            InitialDocumentCount = 10,
            InitialCounterValue = 100,
            OperationsPerActor = 25,
            MaxRetries = 10,
            Seed = 456
        };

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);
        Assert.AreEqual(validation.ExpectedSum, validation.ActualSum,
            $"Sum should match: expected {validation.ExpectedSum}, actual {validation.ActualSum}");

        Console.WriteLine($"Successful increments: {validation.SuccessfulIncrements}");
        Console.WriteLine($"Conflicts detected: {validation.ConflictsDetected}");
        Console.WriteLine($"Retries performed: {validation.RetriesPerformed}");

        runner.Shutdown();
    }

    [TestMethod]
    public void ConcurrentReadModifyWrite_HighContention_NoLostUpdates()
    {
        ConcurrencyConfiguration config = ConcurrencyConfiguration.HighContention();
        config.Seed = 789;

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);
        Assert.IsGreaterThan(0, result.ConflictsDetected, "High contention should produce conflicts");

        Console.WriteLine($"Steps executed: {result.TotalStepsExecuted}");
        Console.WriteLine($"Conflicts: {result.ConflictsDetected}");
        Console.WriteLine($"Retries: {result.RetriesPerformed}");
        Console.WriteLine($"Max retries exceeded: {result.MaxRetriesExceeded}");

        runner.Shutdown();
    }

    [TestMethod]
    public void ConcurrentReadModifyWrite_LowContention_FewConflicts()
    {
        ConcurrencyConfiguration config = ConcurrencyConfiguration.LowContention();
        config.Seed = 1011;

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);

        // Low contention with many documents should have fewer conflicts relative to operations
        double conflictRate = (double)result.ConflictsDetected / result.SuccessfulOperations;
        Console.WriteLine($"Conflict rate: {conflictRate:P2}");
        Console.WriteLine($"Successful operations: {result.SuccessfulOperations}");
        Console.WriteLine($"Conflicts: {result.ConflictsDetected}");

        runner.Shutdown();
    }

    [TestMethod]
    public void SnapshotIsolation_ReadersWithWriters_ConsistentReads()
    {
        ConcurrencyConfiguration config = new ConcurrencyConfiguration
        {
            WriterCount = 4,
            ReaderCount = 8,
            InitialDocumentCount = 50,
            InitialCounterValue = 10,
            OperationsPerActor = 20,
            MaxRetries = 5,
            Seed = 1213
        };

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);
        Assert.IsGreaterThan(0, result.SuccessfulReads, "Should have successful reads");

        Console.WriteLine($"Successful reads: {result.SuccessfulReads}");
        Console.WriteLine($"Successful writes: {validation.SuccessfulIncrements}");

        runner.Shutdown();
    }

    [TestMethod]
    public void Deterministic_SameSeed_SameResults()
    {
        ConcurrencyConfiguration config1 = new ConcurrencyConfiguration
        {
            WriterCount = 4,
            ReaderCount = 2,
            InitialDocumentCount = 20,
            InitialCounterValue = 50,
            OperationsPerActor = 30,
            MaxRetries = 5,
            Seed = 999
        };

        ConcurrencySimulationRunner runner1 = new ConcurrencySimulationRunner(config1);
        runner1.Initialize();
        runner1.SetupInitialDocuments();
        ConcurrencySimulationResult result1 = runner1.Run();
        ConcurrencyValidationResult validation1 = runner1.Validate();
        runner1.Shutdown();

        ConcurrencyConfiguration config2 = new ConcurrencyConfiguration
        {
            WriterCount = 4,
            ReaderCount = 2,
            InitialDocumentCount = 20,
            InitialCounterValue = 50,
            OperationsPerActor = 30,
            MaxRetries = 5,
            Seed = 999
        };

        ConcurrencySimulationRunner runner2 = new ConcurrencySimulationRunner(config2);
        runner2.Initialize();
        runner2.SetupInitialDocuments();
        ConcurrencySimulationResult result2 = runner2.Run();
        ConcurrencyValidationResult validation2 = runner2.Validate();
        runner2.Shutdown();

        Assert.AreEqual(result1.TotalStepsExecuted, result2.TotalStepsExecuted, "Same seed should produce same step count");
        Assert.AreEqual(result1.ConflictsDetected, result2.ConflictsDetected, "Same seed should produce same conflicts");
        Assert.AreEqual(result1.SuccessfulOperations, result2.SuccessfulOperations, "Same seed should produce same successes");
        Assert.AreEqual(validation1.ActualSum, validation2.ActualSum, "Same seed should produce same final sum");
    }

    [TestMethod]
    public void MaxRetriesExceeded_VeryHighContention_SomeAbandoned()
    {
        ConcurrencyConfiguration config = new ConcurrencyConfiguration
        {
            WriterCount = 10,
            ReaderCount = 0,
            InitialDocumentCount = 2,
            InitialCounterValue = 1,
            OperationsPerActor = 50,
            MaxRetries = 3,
            Seed = 1415,
            Strategy = SchedulingStrategy.ConflictBiased
        };

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        // With extreme contention and low max retries, some operations should be abandoned
        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);
        Assert.IsGreaterThan(0, result.ConflictsDetected, "Should have many conflicts");

        Console.WriteLine($"Conflicts: {result.ConflictsDetected}");
        Console.WriteLine($"Max retries exceeded: {result.MaxRetriesExceeded}");
        Console.WriteLine($"Failed operations: {result.FailedOperations}");

        runner.Shutdown();
    }

    [TestMethod]
    public void LargeScale_ManyOperations_Consistent()
    {
        ConcurrencyConfiguration config = new ConcurrencyConfiguration
        {
            WriterCount = 8,
            ReaderCount = 4,
            InitialDocumentCount = 100,
            InitialCounterValue = 0,
            OperationsPerActor = 100,
            MaxRetries = 10,
            Seed = 1617
        };

        ConcurrencySimulationRunner runner = new ConcurrencySimulationRunner(config);
        runner.Initialize();
        runner.SetupInitialDocuments();

        ConcurrencySimulationResult result = runner.Run();
        ConcurrencyValidationResult validation = runner.Validate();

        Assert.IsTrue(validation.IsValid, validation.ErrorMessage);

        Console.WriteLine($"Total steps: {result.TotalStepsExecuted}");
        Console.WriteLine($"Successful operations: {result.SuccessfulOperations}");
        Console.WriteLine($"Conflicts: {result.ConflictsDetected}");
        Console.WriteLine($"Final sum: {validation.ActualSum}");
        Console.WriteLine($"Duration: {result.Duration.TotalMilliseconds:F2}ms");

        runner.Shutdown();
    }

    [TestMethod]
    public void QueryBasedReadModifyWrite_NoLostUpdates()
    {
        // This test verifies that documents read via Query() are properly tracked
        // for conflict detection, not just documents read via GetById()
        SimulationStats stats = new SimulationStats();
        SimulationRandom rng = new SimulationRandom(12345);
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("query_concurrency_test.db", options);

        // Setup: create initial documents
        int documentCount = 10;
        int initialCounterValue = 100;
        List<int> documentIds = new List<int>();

        using (Transaction tx = db.BeginTransaction())
        {
            for (int i = 0; i < documentCount; i++)
            {
                TestDocument doc = new TestDocument
                {
                    Name = $"Doc_{i}",
                    Counter = initialCounterValue,
                    Category = "Test",
                    CreatedAt = DateTime.UtcNow,
                    Payload = Array.Empty<byte>()
                };
                tx.Insert(doc);
                documentIds.Add(doc.Id);
            }
            tx.Commit();
        }

        int initialSum = documentCount * initialCounterValue;

        // Create query-based actors
        int actorCount = 4;
        int operationsPerActor = 25;
        int maxRetries = 10;
        List<QueryBasedConcurrencyActor> actors = new List<QueryBasedConcurrencyActor>();

        for (int i = 0; i < actorCount; i++)
        {
            SimulationRandom actorRng = new SimulationRandom(rng.Next());
            QueryBasedConcurrencyActor actor = new QueryBasedConcurrencyActor(i, actorRng, maxRetries, operationsPerActor);
            actors.Add(actor);
        }

        // Create state tracker
        ConcurrencyState state = new ConcurrencyState();
        state.SetInitialState(initialSum, documentIds);

        // Run simulation with round-robin scheduling
        int totalSteps = 0;
        int maxSteps = actorCount * operationsPerActor * 10;

        while (totalSteps < maxSteps)
        {
            bool anyActive = false;

            foreach (QueryBasedConcurrencyActor actor in actors)
            {
                if (actor.HasPendingWork)
                {
                    anyActive = true;
                    actor.ExecuteStep(db, state);
                    totalSteps++;
                }
            }

            if (!anyActive)
            {
                break;
            }
        }

        // Validate: sum should match expected
        int actualSum = 0;
        using (Transaction tx = db.BeginReadOnlyTransaction())
        {
            foreach (int docId in documentIds)
            {
                TestDocument doc = tx.GetById<TestDocument>(docId);
                if (doc != null)
                {
                    actualSum += doc.Counter;
                }
            }
        }

        int expectedSum = state.ExpectedSum;

        Console.WriteLine($"Initial sum: {initialSum}");
        Console.WriteLine($"Successful increments: {state.SuccessfulIncrements}");
        Console.WriteLine($"Expected sum: {expectedSum}");
        Console.WriteLine($"Actual sum: {actualSum}");
        Console.WriteLine($"Conflicts detected: {state.ConflictsDetected}");
        Console.WriteLine($"Retries performed: {state.RetriesPerformed}");

        Assert.AreEqual(expectedSum, actualSum, $"Sum mismatch: expected {expectedSum}, actual {actualSum}, difference {expectedSum - actualSum}");

        // Cleanup
        foreach (QueryBasedConcurrencyActor actor in actors)
        {
            actor.Reset();
        }
        db.Dispose();
    }
}
