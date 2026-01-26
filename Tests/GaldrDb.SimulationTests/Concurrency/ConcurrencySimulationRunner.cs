using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencySimulationRunner
{
    private readonly SimulationPageIO _pageIO;
    private readonly SimulationWalStream _walStream;
    private readonly SimulationWalStreamIO _walStreamIO;
    private readonly SimulationRandom _rng;
    private readonly SimulationStats _stats;
    private readonly ConcurrencyConfiguration _config;
    private readonly ConcurrencyState _state;
    private readonly ConcurrencyScheduler _scheduler;
    private readonly List<ConcurrencyActor> _actors;

    private GaldrDbEngine.GaldrDb _db;

    public ConcurrencySimulationRunner(ConcurrencyConfiguration config)
    {
        _config = config;
        _stats = new SimulationStats();
        _rng = new SimulationRandom(config.Seed);
        _pageIO = new SimulationPageIO(8192, _stats);
        _walStream = new SimulationWalStream(_stats);
        _walStreamIO = new SimulationWalStreamIO(_walStream);
        _state = new ConcurrencyState();
        _scheduler = new ConcurrencyScheduler(_rng, config.Strategy);
        _actors = new List<ConcurrencyActor>();
    }

    public ConcurrencyState State => _state;
    public SimulationStats Stats => _stats;

    public void Initialize()
    {
        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = _pageIO,
            CustomWalStreamIO = _walStreamIO,
            CustomWalSaltGenerator = () => _rng.NextUInt()
        };

        _db = GaldrDbEngine.GaldrDb.Create("concurrency_simulation.db", options);

        CreateActors();
    }

    private void CreateActors()
    {
        int actorId = 0;

        for (int i = 0; i < _config.WriterCount; i++)
        {
            SimulationRandom actorRng = new SimulationRandom(_rng.Next());
            ConcurrencyActor writer = new ConcurrencyActor(
                actorId++,
                actorRng,
                isReader: false,
                _config.MaxRetries,
                _config.OperationsPerActor
            );
            _actors.Add(writer);
            _scheduler.RegisterActor(writer);
        }

        for (int i = 0; i < _config.ReaderCount; i++)
        {
            SimulationRandom actorRng = new SimulationRandom(_rng.Next());
            ConcurrencyActor reader = new ConcurrencyActor(
                actorId++,
                actorRng,
                isReader: true,
                _config.MaxRetries,
                _config.OperationsPerActor
            );
            _actors.Add(reader);
            _scheduler.RegisterActor(reader);
        }
    }

    public void SetupInitialDocuments()
    {
        List<int> documentIds = new List<int>();
        int initialSum = 0;

        using (Transaction tx = _db.BeginTransaction())
        {
            for (int i = 0; i < _config.InitialDocumentCount; i++)
            {
                TestDocument doc = new TestDocument
                {
                    Name = $"Doc_{i}",
                    Counter = _config.InitialCounterValue,
                    Category = "Test",
                    CreatedAt = DateTime.UtcNow,
                    Payload = Array.Empty<byte>()
                };

                tx.Insert(doc);
                documentIds.Add(doc.Id);
                initialSum += _config.InitialCounterValue;
            }

            tx.Commit();
        }

        _state.SetInitialState(initialSum, documentIds);
    }

    public ConcurrencySimulationResult Run()
    {
        DateTime startTime = DateTime.UtcNow;
        int totalSteps = 0;
        int successfulOps = 0;
        int failedOps = 0;

        while (_scheduler.HasActiveActors())
        {
            ConcurrencyActor actor = _scheduler.SelectNextActor();
            if (actor == null)
            {
                break;
            }

            ActorStepResult stepResult = actor.ExecuteStep(_db, _state);
            totalSteps++;

            if (stepResult.OperationCompleted)
            {
                if (stepResult.Success)
                {
                    successfulOps++;
                }
                else
                {
                    failedOps++;
                }
            }
        }

        DateTime endTime = DateTime.UtcNow;

        return new ConcurrencySimulationResult
        {
            TotalStepsExecuted = totalSteps,
            SuccessfulOperations = successfulOps,
            FailedOperations = failedOps,
            ConflictsDetected = _state.ConflictsDetected,
            RetriesPerformed = _state.RetriesPerformed,
            MaxRetriesExceeded = _state.MaxRetriesExceeded,
            SuccessfulReads = _state.SuccessfulReads,
            Duration = endTime - startTime
        };
    }

    public ConcurrencyValidationResult Validate()
    {
        int actualSum = 0;

        using (Transaction tx = _db.BeginReadOnlyTransaction())
        {
            List<int> docIds = _state.GetAllDocumentIds();
            foreach (int docId in docIds)
            {
                TestDocument doc = tx.GetById<TestDocument>(docId);
                if (doc != null)
                {
                    actualSum += doc.Counter;
                }
            }
        }

        int expectedSum = _state.ExpectedSum;
        bool isValid = actualSum == expectedSum;

        string errorMessage = null;
        if (!isValid)
        {
            errorMessage = $"Sum mismatch: expected {expectedSum}, actual {actualSum}, difference {expectedSum - actualSum}";
        }

        return new ConcurrencyValidationResult
        {
            IsValid = isValid,
            ExpectedSum = expectedSum,
            ActualSum = actualSum,
            SuccessfulIncrements = _state.SuccessfulIncrements,
            ConflictsDetected = _state.ConflictsDetected,
            RetriesPerformed = _state.RetriesPerformed,
            ErrorMessage = errorMessage
        };
    }

    public void Shutdown()
    {
        foreach (ConcurrencyActor actor in _actors)
        {
            actor.Reset();
        }

        if (_db != null)
        {
            _db.Dispose();
            _db = null;
        }
    }
}
