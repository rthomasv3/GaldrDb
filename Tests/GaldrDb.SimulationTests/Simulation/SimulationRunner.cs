using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDb.SimulationTests.Workload.Operations;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Simulation;

public class SimulationRunner
{
    private readonly SimulationPageIO _pageIO;
    private readonly SimulationWalStream _walStream;
    private readonly SimulationWalStreamIO _walStreamIO;
    private readonly SimulationRandom _rng;
    private readonly SimulationStats _stats;
    private readonly WorkloadConfiguration _config;
    private readonly SimulationState _state;
    private readonly WorkloadGenerator _workloadGenerator;

    private GaldrDbEngine.GaldrDb _db;
    private bool _initialized;

    public SimulationRunner(WorkloadConfiguration config, int seed)
    {
        _config = config;
        _stats = new SimulationStats();
        _rng = new SimulationRandom(seed);
        _pageIO = new SimulationPageIO(8192, _stats);
        _walStream = new SimulationWalStream(_stats);
        _walStreamIO = new SimulationWalStreamIO(_walStream);
        _state = new SimulationState();
        _workloadGenerator = new WorkloadGenerator(config, _rng);
        _initialized = false;
    }

    public SimulationStats Stats => _stats;
    public SimulationState State => _state;
    public bool IsInitialized => _initialized;

    public void Initialize()
    {
        GaldrDbOptions options = CreateOptions();

        _db = GaldrDbEngine.GaldrDb.Create("simulation.db", options);
        _initialized = true;

        // Ensure collections exist
        foreach (string collectionName in _workloadGenerator.CollectionNames)
        {
            _state.EnsureCollection(collectionName);
        }
    }

    private GaldrDbOptions CreateOptions()
    {
        return new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = _pageIO,
            CustomWalStreamIO = _walStreamIO,
            CustomWalSaltGenerator = () => _rng.NextUInt()
        };
    }

    /// <summary>
    /// Simulates a crash by discarding all unflushed data and disposing the database.
    /// Call Recover() after this to reopen the database.
    /// </summary>
    public void SimulateCrash()
    {
        // Dispose database without clean shutdown (simulating crash)
        if (_db != null)
        {
            // Don't call _db.Dispose() as that would flush - just abandon it
            _db = null;
        }

        // Discard unflushed data
        _pageIO.SimulateCrash();
        _walStream.SimulateCrash();
    }

    /// <summary>
    /// Recovers the database after a crash by reopening it.
    /// This triggers WAL recovery which replays committed transactions.
    /// </summary>
    public void Recover()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException("Cannot recover before Initialize() has been called");
        }

        GaldrDbOptions options = CreateOptions();
        _db = GaldrDbEngine.GaldrDb.Open("simulation.db", options);
    }

    public SimulationResult Run()
    {
        SimulationResult result = new SimulationResult();
        result.StartTime = DateTime.UtcNow;

        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < _config.OperationCount; i++)
        {
            Operation op = _workloadGenerator.GenerateOperation(_state);

            using (Transaction tx = _db.BeginTransaction())
            {
                OperationResult opResult = op.Execute(_db, tx, _state);

                if (opResult.Success)
                {
                    tx.Commit();
                    successCount++;

                    // Update state based on operation type
                    if (op is InsertOperation && opResult.DocId.HasValue)
                    {
                        _state.RecordInsert(op.CollectionName, opResult.DocId.Value, opResult.ContentHash, tx.TxId.Value);
                    }
                    else if (op is UpdateOperation updateOp && opResult.DocId.HasValue)
                    {
                        _state.RecordUpdate(op.CollectionName, updateOp.DocId, opResult.ContentHash, tx.TxId.Value);
                    }
                    else if (op is DeleteOperation deleteOp)
                    {
                        _state.RecordDelete(op.CollectionName, deleteOp.DocId);
                    }
                }
                else
                {
                    failureCount++;
                }
            }

            _stats.OperationsExecuted++;
        }

        result.EndTime = DateTime.UtcNow;
        result.SuccessCount = successCount;
        result.FailureCount = failureCount;
        result.TotalOperations = _config.OperationCount;
        result.FinalDocumentCount = _state.GetTotalDocumentCount();

        return result;
    }

    public void Shutdown()
    {
        if (_db != null)
        {
            _db.Dispose();
            _db = null;
        }
    }

    public bool VerifyState()
    {
        bool allValid = true;

        foreach (string collection in _state.GetCollectionNames())
        {
            List<int> expectedIds = _state.GetAllDocumentIds(collection);

            using (Transaction tx = _db.BeginReadOnlyTransaction())
            {
                foreach (int docId in expectedIds)
                {
                    TestDocument doc = tx.GetById<TestDocument>(docId);

                    if (doc == null)
                    {
                        allValid = false;
                        break;
                    }

                    byte[] actualHash = doc.ComputeHash();
                    if (!_state.VerifyDocumentHash(collection, docId, actualHash))
                    {
                        allValid = false;
                        break;
                    }
                }
            }

            if (!allValid)
            {
                break;
            }
        }

        return allValid;
    }
}

public class SimulationResult
{
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public int SuccessCount { get; set; }
    public int FailureCount { get; set; }
    public int TotalOperations { get; set; }
    public int FinalDocumentCount { get; set; }

    public TimeSpan Duration => EndTime - StartTime;
    public double OperationsPerSecond => TotalOperations / Duration.TotalSeconds;
}
