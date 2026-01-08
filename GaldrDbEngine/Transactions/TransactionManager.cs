using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Transactions;

public class TransactionManager
{
    private readonly object _lock;
    private readonly HashSet<TxId> _activeTransactionIds;
    private TxId _nextTxId;
    private TxId _lastCommittedTxId;

    public TransactionManager()
    {
        _lock = new object();
        _activeTransactionIds = new HashSet<TxId>();
        _nextTxId = new TxId(1);
        _lastCommittedTxId = TxId.None;
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

    public void RegisterTransaction(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Add(txId);
        }
    }

    public void UnregisterTransaction(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
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
        }
    }

    public void MarkAborted(TxId txId)
    {
        lock (_lock)
        {
            _activeTransactionIds.Remove(txId);
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
