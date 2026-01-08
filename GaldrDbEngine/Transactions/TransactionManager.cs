using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Transactions;

public class TransactionManager
{
    private readonly object _lock;
    private readonly HashSet<TxId> _activeTransactionIds;
    private readonly Dictionary<TxId, TxId> _activeSnapshots; // TxId -> SnapshotTxId
    private TxId _nextTxId;
    private TxId _lastCommittedTxId;
    private long _commitCount;

    public TransactionManager()
    {
        _lock = new object();
        _activeTransactionIds = new HashSet<TxId>();
        _activeSnapshots = new Dictionary<TxId, TxId>();
        _nextTxId = new TxId(1);
        _lastCommittedTxId = TxId.None;
        _commitCount = 0;
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

    public void RegisterTransaction(TxId txId, TxId snapshotTxId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Add(txId);
            _activeSnapshots[txId] = snapshotTxId;
        }
    }

    public void UnregisterTransaction(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
            _activeSnapshots.Remove(txId);
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
            _commitCount++;
        }
    }

    public void MarkAborted(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
            _activeSnapshots.Remove(txId);
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
}
