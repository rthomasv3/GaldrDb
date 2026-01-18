using System.Collections.Generic;

namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencyState
{
    private readonly object _lock;
    private int _initialSum;
    private int _successfulIncrements;
    private int _conflictsDetected;
    private int _retriesPerformed;
    private int _maxRetriesExceeded;
    private int _successfulReads;
    private readonly List<int> _documentIds;

    public ConcurrencyState()
    {
        _lock = new object();
        _documentIds = new List<int>();
        _initialSum = 0;
        _successfulIncrements = 0;
        _conflictsDetected = 0;
        _retriesPerformed = 0;
        _maxRetriesExceeded = 0;
        _successfulReads = 0;
    }

    public void SetInitialState(int initialSum, List<int> documentIds)
    {
        lock (_lock)
        {
            _initialSum = initialSum;
            _documentIds.Clear();
            _documentIds.AddRange(documentIds);
        }
    }

    public void RecordSuccessfulIncrement()
    {
        lock (_lock)
        {
            _successfulIncrements++;
        }
    }

    public void RecordConflict()
    {
        lock (_lock)
        {
            _conflictsDetected++;
        }
    }

    public void RecordRetry()
    {
        lock (_lock)
        {
            _retriesPerformed++;
        }
    }

    public void RecordMaxRetriesExceeded()
    {
        lock (_lock)
        {
            _maxRetriesExceeded++;
        }
    }

    public void RecordSuccessfulRead()
    {
        lock (_lock)
        {
            _successfulReads++;
        }
    }

    public int GetRandomDocumentId(GaldrDb.SimulationTests.Core.SimulationRandom rng)
    {
        lock (_lock)
        {
            int index = rng.Next(_documentIds.Count);
            return _documentIds[index];
        }
    }

    public List<int> GetAllDocumentIds()
    {
        lock (_lock)
        {
            return new List<int>(_documentIds);
        }
    }

    public int InitialSum
    {
        get
        {
            lock (_lock)
            {
                return _initialSum;
            }
        }
    }

    public int SuccessfulIncrements
    {
        get
        {
            lock (_lock)
            {
                return _successfulIncrements;
            }
        }
    }

    public int ConflictsDetected
    {
        get
        {
            lock (_lock)
            {
                return _conflictsDetected;
            }
        }
    }

    public int RetriesPerformed
    {
        get
        {
            lock (_lock)
            {
                return _retriesPerformed;
            }
        }
    }

    public int MaxRetriesExceeded
    {
        get
        {
            lock (_lock)
            {
                return _maxRetriesExceeded;
            }
        }
    }

    public int SuccessfulReads
    {
        get
        {
            lock (_lock)
            {
                return _successfulReads;
            }
        }
    }

    public int ExpectedSum
    {
        get
        {
            lock (_lock)
            {
                return _initialSum + _successfulIncrements;
            }
        }
    }
}
