using System;
using System.Diagnostics;
using System.Threading;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;

namespace GaldrDbConsole.StressTest;

public class StressTestWorker
{
    private readonly int _workerId;
    private readonly GaldrDb _db;
    private readonly StressTestState _state;
    private readonly StressTestStatistics _stats;
    private readonly WorkloadWeights _weights;
    private readonly int _maxRetries;
    private readonly Random _rng;
    private readonly CancellationToken _cancellationToken;

    private volatile bool _shouldStop;
    private Exception _fatalError;
    private int _validationErrorDocId;

    public StressTestWorker(
        int workerId,
        GaldrDb db,
        StressTestState state,
        StressTestStatistics stats,
        WorkloadWeights weights,
        int maxRetries,
        int seed,
        CancellationToken cancellationToken)
    {
        _workerId = workerId;
        _db = db;
        _state = state;
        _stats = stats;
        _weights = weights;
        _maxRetries = maxRetries;
        _rng = new Random(seed + workerId);
        _cancellationToken = cancellationToken;
        _shouldStop = false;
        _fatalError = null;
        _validationErrorDocId = -1;
    }

    public int WorkerId => _workerId;
    public bool HasFatalError => _fatalError != null;
    public Exception FatalError => _fatalError;
    public bool HasValidationError => _validationErrorDocId >= 0;
    public int ValidationErrorDocId => _validationErrorDocId;

    public void Run()
    {
        while (!_shouldStop && !_cancellationToken.IsCancellationRequested)
        {
            try
            {
                ExecuteOperation();
            }
            catch (Exception ex)
            {
                _fatalError = ex;
                _stats.RecordUnexpectedError();
                break;
            }
        }
    }

    public void Stop()
    {
        _shouldStop = true;
    }

    private void ExecuteOperation()
    {
        OperationType opType = SelectOperation();

        switch (opType)
        {
            case OperationType.Insert:
                ExecuteInsert();
                break;
            case OperationType.Read:
                ExecuteRead();
                break;
            case OperationType.Update:
                ExecuteUpdate();
                break;
            case OperationType.Delete:
                ExecuteDelete();
                break;
        }
    }

    private OperationType SelectOperation()
    {
        int roll = _rng.Next(_weights.TotalWeight());
        OperationType opType;

        if (roll < _weights.InsertWeight)
        {
            opType = OperationType.Insert;
        }
        else if (roll < _weights.InsertWeight + _weights.ReadWeight)
        {
            opType = OperationType.Read;
        }
        else if (roll < _weights.InsertWeight + _weights.ReadWeight + _weights.UpdateWeight)
        {
            opType = OperationType.Update;
        }
        else
        {
            opType = OperationType.Delete;
        }

        int? randomDocId = _state.GetRandomDocumentId(_rng);
        if (randomDocId == null && opType != OperationType.Insert)
        {
            opType = OperationType.Insert;
        }

        return opType;
    }

    private void ExecuteInsert()
    {
        Stopwatch sw = Stopwatch.StartNew();
        bool success = false;

        StressTestDocument doc = StressTestDocument.Generate(_rng, _workerId);

        int retryCount = 0;
        while (retryCount <= _maxRetries && !_shouldStop && !_cancellationToken.IsCancellationRequested)
        {
            try
            {
                using (ITransaction tx = _db.BeginTransaction())
                {
                    int docId = tx.Insert(doc);
                    tx.Commit();

                    doc.Id = docId;
                    byte[] actualHash = doc.ComputeHash();
                    _state.RecordInsert(docId, actualHash, doc.Version);

                    success = true;
                }
                break;
            }
            catch (WriteConflictException)
            {
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("page conflict"))
            {
                // Page-level conflict exhaustion - treat as retryable conflict
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
        }

        sw.Stop();
        _stats.RecordInsert(success, sw.Elapsed.TotalMilliseconds);
    }

    private void ExecuteRead()
    {
        Stopwatch sw = Stopwatch.StartNew();
        bool success = false;

        int? docId = _state.GetRandomDocumentId(_rng);
        if (docId == null)
        {
            sw.Stop();
            _stats.RecordRead(false, sw.Elapsed.TotalMilliseconds);
            return;
        }

        byte[] expectedHash = _state.GetExpectedHash(docId.Value);

        using (ITransaction tx = _db.BeginReadOnlyTransaction())
        {
            StressTestDocument doc = tx.GetById<StressTestDocument>(docId.Value);

            if (doc != null)
            {
                byte[] actualHash = doc.ComputeHash();

                if (expectedHash != null)
                {
                    bool hashMatch = _state.VerifyHash(docId.Value, actualHash);
                    if (!hashMatch)
                    {
                        _stats.RecordValidationError();
                        _validationErrorDocId = docId.Value;
                        _shouldStop = true;
                    }
                    else
                    {
                        success = true;
                    }
                }
                else
                {
                    success = true;
                }
            }
            else
            {
                success = true;
            }
        }

        sw.Stop();
        _stats.RecordRead(success, sw.Elapsed.TotalMilliseconds);
    }

    private void ExecuteUpdate()
    {
        Stopwatch sw = Stopwatch.StartNew();
        bool success = false;

        int? docId = _state.GetRandomDocumentId(_rng);
        if (docId == null)
        {
            sw.Stop();
            _stats.RecordUpdate(false, sw.Elapsed.TotalMilliseconds);
            return;
        }

        int retryCount = 0;
        while (retryCount <= _maxRetries && !_shouldStop && !_cancellationToken.IsCancellationRequested)
        {
            try
            {
                using (ITransaction tx = _db.BeginTransaction())
                {
                    StressTestDocument existing = tx.GetById<StressTestDocument>(docId.Value);

                    if (existing != null)
                    {
                        int newCounter = existing.Counter + 1;
                        DateTime newUpdatedAt = DateTime.UtcNow;
                        string newCategory = CATEGORIES[_rng.Next(CATEGORIES.Length)];
                        int newVersion = existing.Version + 1;

                        tx.UpdateById<StressTestDocument>(docId.Value)
                            .Set(StressTestDocumentMeta.Counter, newCounter)
                            .Set(StressTestDocumentMeta.UpdatedAt, newUpdatedAt)
                            .Set(StressTestDocumentMeta.Category, newCategory)
                            .Set(StressTestDocumentMeta.WorkerId, _workerId)
                            .Set(StressTestDocumentMeta.Version, newVersion)
                            .Execute();

                        tx.Commit();

                        existing.Counter = newCounter;
                        existing.UpdatedAt = newUpdatedAt;
                        existing.Category = newCategory;
                        existing.WorkerId = _workerId;
                        existing.Version = newVersion;

                        byte[] newHash = existing.ComputeHash();
                        _state.RecordUpdate(docId.Value, newHash, newVersion);

                        success = true;
                    }
                    else
                    {
                        success = true;
                    }
                }
                break;
            }
            catch (WriteConflictException)
            {
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("page conflict"))
            {
                // Page-level conflict exhaustion - treat as retryable conflict
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
        }

        sw.Stop();
        _stats.RecordUpdate(success, sw.Elapsed.TotalMilliseconds);
    }

    private void ExecuteDelete()
    {
        Stopwatch sw = Stopwatch.StartNew();
        bool success = false;

        int? docId = _state.GetRandomDocumentId(_rng);
        if (docId == null)
        {
            sw.Stop();
            _stats.RecordDelete(false, sw.Elapsed.TotalMilliseconds);
            return;
        }

        int retryCount = 0;
        while (retryCount <= _maxRetries && !_shouldStop && !_cancellationToken.IsCancellationRequested)
        {
            try
            {
                using (ITransaction tx = _db.BeginTransaction())
                {
                    bool deleted = tx.DeleteById<StressTestDocument>(docId.Value);
                    tx.Commit();

                    if (deleted)
                    {
                        _state.RecordDelete(docId.Value);
                    }

                    success = true;
                }
                break;
            }
            catch (WriteConflictException)
            {
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("page conflict"))
            {
                // Page-level conflict exhaustion - treat as retryable conflict
                _stats.RecordWriteConflict();
                retryCount++;

                if (retryCount > _maxRetries)
                {
                    _stats.RecordMaxRetriesExceeded();
                }
                else
                {
                    _stats.RecordRetry();
                }
            }
        }

        sw.Stop();
        _stats.RecordDelete(success, sw.Elapsed.TotalMilliseconds);
    }

    private static readonly string[] CATEGORIES = { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };
}

public enum OperationType
{
    Insert,
    Read,
    Update,
    Delete
}
