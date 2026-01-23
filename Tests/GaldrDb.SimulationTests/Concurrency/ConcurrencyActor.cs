using System;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencyActor
{
    private readonly int _actorId;
    private readonly SimulationRandom _rng;
    private readonly bool _isReader;
    private readonly int _maxRetries;
    private readonly int _maxOperations;

    private ActorState _state;
    private Transaction _currentTransaction;
    private int _currentDocumentId;
    private int _readCounterValue;
    private int _retryCount;
    private int _operationsCompleted;

    public ConcurrencyActor(int actorId, SimulationRandom rng, bool isReader, int maxRetries, int maxOperations)
    {
        _actorId = actorId;
        _rng = rng;
        _isReader = isReader;
        _maxRetries = maxRetries;
        _maxOperations = maxOperations;
        _state = ActorState.Idle;
        _currentTransaction = null;
        _currentDocumentId = 0;
        _readCounterValue = 0;
        _retryCount = 0;
        _operationsCompleted = 0;
    }

    public int ActorId => _actorId;
    public ActorState State => _state;
    public bool IsReader => _isReader;
    public int RetryCount => _retryCount;
    public int OperationsCompleted => _operationsCompleted;
    public int CurrentDocumentId => _currentDocumentId;
    public bool HasPendingWork => _operationsCompleted < _maxOperations;
    public bool IsIdle => _state == ActorState.Idle;

    public ActorStepResult ExecuteStep(GaldrDbEngine.GaldrDb db, ConcurrencyState state)
    {
        ActorStepResult result;

        if (_isReader)
        {
            result = ExecuteReaderStep(db, state);
        }
        else
        {
            result = ExecuteWriterStep(db, state);
        }

        return result;
    }

    private ActorStepResult ExecuteReaderStep(GaldrDbEngine.GaldrDb db, ConcurrencyState state)
    {
        ActorStepResult result;

        switch (_state)
        {
            case ActorState.Idle:
                _currentDocumentId = state.GetRandomDocumentId(_rng);
                _currentTransaction = db.BeginReadOnlyTransaction();
                _state = ActorState.ReadingDocument;
                result = ActorStepResult.Continue();
                break;

            case ActorState.ReadingDocument:
                TestDocument doc = _currentTransaction.GetById<TestDocument>(_currentDocumentId);
                _currentTransaction.Dispose();
                _currentTransaction = null;

                if (doc != null)
                {
                    state.RecordSuccessfulRead();
                }

                _operationsCompleted++;
                _state = ActorState.Idle;
                result = ActorStepResult.Completed(0);
                break;

            default:
                result = ActorStepResult.Failed();
                break;
        }

        return result;
    }

    private ActorStepResult ExecuteWriterStep(GaldrDbEngine.GaldrDb db, ConcurrencyState state)
    {
        ActorStepResult result;

        switch (_state)
        {
            case ActorState.Idle:
                _currentDocumentId = state.GetRandomDocumentId(_rng);
                _currentTransaction = db.BeginTransaction();
                _state = ActorState.ReadingDocument;
                result = ActorStepResult.Continue();
                break;

            case ActorState.ReadingDocument:
                TestDocument readDoc = _currentTransaction.GetById<TestDocument>(_currentDocumentId);
                if (readDoc != null)
                {
                    _readCounterValue = readDoc.Counter;
                    _state = ActorState.ModifyingDocument;
                    result = ActorStepResult.Continue();
                }
                else
                {
                    _currentTransaction.Dispose();
                    _currentTransaction = null;
                    _operationsCompleted++;
                    _state = ActorState.Idle;
                    result = ActorStepResult.Failed();
                }
                break;

            case ActorState.ModifyingDocument:
                _state = ActorState.Committing;
                result = ActorStepResult.Continue();
                break;

            case ActorState.Committing:
                result = TryCommit(db, state);
                break;

            case ActorState.Retrying:
                _currentDocumentId = state.GetRandomDocumentId(_rng);
                _currentTransaction = db.BeginTransaction();
                _state = ActorState.ReadingDocument;
                state.RecordRetry();
                result = ActorStepResult.Continue();
                break;

            default:
                result = ActorStepResult.Failed();
                break;
        }

        return result;
    }

    private ActorStepResult TryCommit(GaldrDbEngine.GaldrDb db, ConcurrencyState state)
    {
        ActorStepResult result;

        try
        {
            // Create updated document using the value we read earlier
            // This simulates read-modify-write where we read, compute new value, then write
            TestDocument updateDoc = new TestDocument
            {
                Id = _currentDocumentId,
                Counter = _readCounterValue + 1,
                Name = $"Doc_{_currentDocumentId}",
                Category = "Test",
                CreatedAt = DateTime.UtcNow,
                Payload = Array.Empty<byte>()
            };

            bool updated = _currentTransaction.Replace(updateDoc);
            _currentTransaction.Commit();
            _currentTransaction.Dispose();
            _currentTransaction = null;

            if (updated)
            {
                state.RecordSuccessfulIncrement();
            }

            _operationsCompleted++;
            _retryCount = 0;
            _state = ActorState.Idle;
            result = ActorStepResult.Completed(updated ? 1 : 0);
        }
        catch (WriteConflictException)
        {
            _currentTransaction.Dispose();
            _currentTransaction = null;

            state.RecordConflict();
            _retryCount++;

            if (_retryCount >= _maxRetries)
            {
                state.RecordMaxRetriesExceeded();
                _operationsCompleted++;
                _retryCount = 0;
                _state = ActorState.Idle;
                result = ActorStepResult.Failed();
            }
            else
            {
                _state = ActorState.Retrying;
                result = ActorStepResult.Conflict();
            }
        }

        return result;
    }

    public void Reset()
    {
        if (_currentTransaction != null)
        {
            _currentTransaction.Dispose();
            _currentTransaction = null;
        }

        _state = ActorState.Idle;
        _currentDocumentId = 0;
        _readCounterValue = 0;
        _retryCount = 0;
    }
}
