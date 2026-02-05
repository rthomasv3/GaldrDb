using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Transactions;

internal class TransactionManager
{
    private readonly object _lock;
    private readonly HashSet<TxId> _activeTransactionIds;
    private readonly Dictionary<TxId, TxId> _activeSnapshots; // TxId -> SnapshotTxId
    private readonly Dictionary<TxId, ulong> _activeSnapshotCSNs; // TxId -> SnapshotCSN
    private TxId _nextTxId;
    private TxId _lastCommittedTxId;
    private long _commitCount;
    private ulong _commitSequenceNumber;

    public TransactionManager()
    {
        _lock = new object();
        _activeTransactionIds = new HashSet<TxId>();
        _activeSnapshots = new Dictionary<TxId, TxId>();
        _activeSnapshotCSNs = new Dictionary<TxId, ulong>();
        _nextTxId = new TxId(1);
        _lastCommittedTxId = TxId.None;
        _commitCount = 0;
        _commitSequenceNumber = 0;
    }

    public TxId LastCommittedTxId
    {
        get
        {
            lock (_lock)
            {
                return _lastCommittedTxId;
            }
        }
    }

    public int ActiveTransactionCount
    {
        get
        {
            lock (_lock)
            {
                return _activeTransactionIds.Count;
            }
        }
    }

    public TxId AllocateTxId()
    {
        lock (_lock)
        {
            TxId txId = _nextTxId;
            _nextTxId++;
            return txId;
        }
    }

    /// <summary>
    /// Atomically allocates a transaction ID, captures snapshot, and registers the transaction.
    /// This prevents a race where GC could miss the snapshot between GetSnapshotTxId and RegisterTransaction.
    /// </summary>
    public void AllocateAndRegisterTransaction(out TxId txId, out TxId snapshotTxId, out ulong snapshotCSN)
    {
        lock (_lock)
        {
            txId = _nextTxId;
            _nextTxId++;
            snapshotTxId = _lastCommittedTxId;
            snapshotCSN = _commitSequenceNumber;

            _activeTransactionIds.Add(txId);
            _activeSnapshots[txId] = snapshotTxId;
            _activeSnapshotCSNs[txId] = snapshotCSN;
        }
    }

    public void RegisterTransaction(TxId txId, TxId snapshotTxId, ulong snapshotCSN)
    {
        lock (_lock)
        {
            _activeTransactionIds.Add(txId);
            _activeSnapshots[txId] = snapshotTxId;
            _activeSnapshotCSNs[txId] = snapshotCSN;
        }
    }

    public void UnregisterTransaction(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
            _activeSnapshots.Remove(txId);
            _activeSnapshotCSNs.Remove(txId);
        }
    }

    public void MarkCommitted(TxId txId)
    {
        lock (_lock)
        {
            if (txId > _lastCommittedTxId)
            {
                _lastCommittedTxId = txId;
            }

            _activeTransactionIds.Remove(txId);
            _activeSnapshots.Remove(txId);
            _activeSnapshotCSNs.Remove(txId);
            _commitCount++;
        }
    }

    public void MarkAborted(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
            _activeSnapshots.Remove(txId);
            _activeSnapshotCSNs.Remove(txId);
        }
    }

    public long CommitCount
    {
        get
        {
            lock (_lock)
            {
                return _commitCount;
            }
        }
    }

    public TxId GetSnapshotTxId()
    {
        lock (_lock)
        {
            return _lastCommittedTxId;
        }
    }

    public TxId GetOldestActiveTransaction()
    {
        lock (_lock)
        {
            TxId oldest = TxId.MaxValue;

            foreach (TxId txId in _activeTransactionIds)
            {
                if (txId < oldest)
                {
                    oldest = txId;
                }
            }

            return oldest;
        }
    }

    public TxId GetOldestActiveSnapshot()
    {
        lock (_lock)
        {
            TxId oldest = TxId.MaxValue;

            foreach (TxId snapshotTxId in _activeSnapshots.Values)
            {
                if (snapshotTxId < oldest)
                {
                    oldest = snapshotTxId;
                }
            }

            return oldest;
        }
    }

    public bool IsTransactionActive(TxId txId)
    {
        lock (_lock)
        {
            return _activeTransactionIds.Contains(txId);
        }
    }

    public void SetLastCommittedTxId(TxId txId)
    {
        lock (_lock)
        {
            if (txId > _lastCommittedTxId)
            {
                _lastCommittedTxId = txId;
            }

            if (txId >= _nextTxId)
            {
                _nextTxId = new TxId(txId.Value + 1);
            }
        }
    }

    /// <summary>
    /// Gets the next commit sequence number (CSN) and increments the counter.
    /// Called at commit time under _commitSerializationLock.
    /// </summary>
    public ulong GetNextCommitCSN()
    {
        lock (_lock)
        {
            _commitSequenceNumber++;
            return _commitSequenceNumber;
        }
    }

    /// <summary>
    /// Gets the current commit sequence number without incrementing.
    /// </summary>
    public ulong GetCurrentCSN()
    {
        lock (_lock)
        {
            return _commitSequenceNumber;
        }
    }

    /// <summary>
    /// Gets the oldest active snapshot CSN for garbage collection.
    /// Returns ulong.MaxValue if no active snapshots.
    /// </summary>
    public ulong GetOldestActiveSnapshotCSN()
    {
        lock (_lock)
        {
            ulong oldest = ulong.MaxValue;

            foreach (ulong snapshotCSN in _activeSnapshotCSNs.Values)
            {
                if (snapshotCSN < oldest)
                {
                    oldest = snapshotCSN;
                }
            }

            return oldest;
        }
    }

    /// <summary>
    /// Sets the commit sequence number. Used for recovery.
    /// </summary>
    public void SetCommitSequenceNumber(ulong csn)
    {
        lock (_lock)
        {
            if (csn > _commitSequenceNumber)
            {
                _commitSequenceNumber = csn;
            }
        }
    }
}
