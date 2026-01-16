using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

internal class WalPageIO : IPageIO
{
    private readonly IPageIO _innerPageIO;
    private readonly WriteAheadLog _wal;
    private readonly int _pageSize;
    private readonly Dictionary<int, byte[]> _walPageCache;
    private readonly object _cacheLock;

    // Per-transaction WAL write buffers (cache writes are immediate, WAL writes are batched)
    private readonly ConcurrentDictionary<ulong, List<PendingPageWrite>> _txWalWrites;

    // Track current transaction per async flow (thread/task)
    private readonly AsyncLocal<ulong?> _currentTxId;

    // Lock for serializing WAL commits
    private readonly object _commitLock;

    // Reader-writer lock for coordinating base file access during checkpoint
    private readonly ReaderWriterLockSlim _rwLock;

    // Reader-writer lock for transaction/checkpoint coordination
    // Transactions hold read lock, checkpoint holds write lock
    private readonly ReaderWriterLockSlim _checkpointLock;

    private bool _disposed;

    public WalPageIO(IPageIO innerPageIO, WriteAheadLog wal, int pageSize)
    {
        _innerPageIO = innerPageIO;
        _wal = wal;
        _pageSize = pageSize;
        _walPageCache = new Dictionary<int, byte[]>();
        _cacheLock = new object();
        _txWalWrites = new ConcurrentDictionary<ulong, List<PendingPageWrite>>();
        _currentTxId = new AsyncLocal<ulong?>();
        _commitLock = new object();
        _rwLock = new ReaderWriterLockSlim();
        _checkpointLock = new ReaderWriterLockSlim();
        _disposed = false;
    }

    public bool InTransaction
    {
        get { return _currentTxId.Value.HasValue; }
    }

    public void BeginTransaction(ulong txId)
    {
        // Acquire shared lock - blocks if checkpoint is in progress
        _checkpointLock.EnterReadLock();
        _txWalWrites[txId] = new List<PendingPageWrite>();
        _currentTxId.Value = txId;
    }

    public void CommitTransaction()
    {
        ulong? txId = _currentTxId.Value;
        if (!txId.HasValue)
        {
            throw new InvalidOperationException("No transaction in progress");
        }

        if (!_txWalWrites.TryRemove(txId.Value, out List<PendingPageWrite> walWrites))
        {
            throw new InvalidOperationException("Transaction WAL buffer not found");
        }

        // Write all buffered WAL frames atomically
        lock (_commitLock)
        {
            if (walWrites.Count > 0)
            {
                _wal.WriteTransactionBatch(txId.Value, walWrites);
                _wal.Flush();
            }
        }

        // Return WAL buffers to pool
        foreach (PendingPageWrite write in walWrites)
        {
            BufferPool.Return(write.Data);
        }

        walWrites.Clear();
        _currentTxId.Value = null;
        _checkpointLock.ExitReadLock();
    }

    public void AbortTransaction()
    {
        ulong? txId = _currentTxId.Value;
        if (!txId.HasValue)
        {
            return;
        }

        if (_txWalWrites.TryRemove(txId.Value, out List<PendingPageWrite> walWrites))
        {
            // Return WAL buffers to pool
            // Note: Cache has "orphaned" data from this transaction, but that's OK
            // because VersionIndex won't point to it. The data is just wasted space
            // until checkpoint/GC cleans it up.
            foreach (PendingPageWrite write in walWrites)
            {
                BufferPool.Return(write.Data);
            }
            walWrites.Clear();
        }

        _currentTxId.Value = null;
        _checkpointLock.ExitReadLock();
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        // Check shared cache first - contains ALL writes (committed + uncommitted)
        // This ensures concurrent transactions see each other's writes for proper
        // slot allocation, avoiding double-allocation bugs.
        lock (_cacheLock)
        {
            if (_walPageCache.TryGetValue(pageId, out byte[] cachedPage))
            {
                cachedPage.AsSpan().CopyTo(destination);
                return;
            }
        }

        // Fall back to base file
        _rwLock.EnterReadLock();
        try
        {
            _innerPageIO.ReadPage(pageId, destination);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        byte[] cacheData = BufferPool.Rent(_pageSize);
        data.CopyTo(cacheData);
        byte pageType = DeterminePageType(cacheData);

        // Check if we're in a transaction (which already holds _checkpointLock)
        ulong? txId = _currentTxId.Value;
        if (txId.HasValue && _txWalWrites.TryGetValue(txId.Value, out List<PendingPageWrite> walWrites))
        {
            // Transaction already holds _checkpointLock read lock from BeginTransaction
            lock (_cacheLock)
            {
                if (_walPageCache.TryGetValue(pageId, out byte[] oldBuffer))
                {
                    BufferPool.Return(oldBuffer);
                }
                _walPageCache[pageId] = cacheData;
            }

            // Make a separate copy for WAL buffer (cache owns cacheData)
            byte[] walData = BufferPool.Rent(_pageSize);
            data.CopyTo(walData);
            walWrites.Add(new PendingPageWrite(pageId, walData, pageType));
        }
        else
        {
            // No active transaction - acquire lock for auto-commit write
            _checkpointLock.EnterReadLock();
            try
            {
                lock (_cacheLock)
                {
                    if (_walPageCache.TryGetValue(pageId, out byte[] oldBuffer))
                    {
                        BufferPool.Return(oldBuffer);
                    }
                    _walPageCache[pageId] = cacheData;
                }

                lock (_commitLock)
                {
                    _wal.WriteFrame(0, pageId, pageType, cacheData, WalFrameFlags.Commit);
                }
            }
            finally
            {
                _checkpointLock.ExitReadLock();
            }
        }
    }

    public void Flush()
    {
        _wal.Flush();
        _innerPageIO.Flush();
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        // Check shared cache first
        lock (_cacheLock)
        {
            if (_walPageCache.TryGetValue(pageId, out byte[] cachedPage))
            {
                cachedPage.AsMemory().CopyTo(destination);
                return;
            }
        }

        // Fall back to base file
        _rwLock.EnterReadLock();
        try
        {
            await _innerPageIO.ReadPageAsync(pageId, destination, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        WritePage(pageId, data.Span);
        return Task.CompletedTask;
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        _wal.Flush();
        await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Close()
    {
        _innerPageIO.Close();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _checkpointLock.Dispose();
            _rwLock.Dispose();
            _innerPageIO.Dispose();
        }
    }

    public bool Checkpoint()
    {
        bool checkpointCompleted = false;
        List<KeyValuePair<int, byte[]>> pagesToWrite = null;

        // Acquire exclusive lock - waits for all active transactions to complete
        _checkpointLock.EnterWriteLock();
        try
        {
            lock (_cacheLock)
            {
                if (_walPageCache.Count > 0)
                {
                    pagesToWrite = new List<KeyValuePair<int, byte[]>>(_walPageCache);
                }
            }

            if (pagesToWrite != null)
            {
                _rwLock.EnterWriteLock();
                try
                {
                    foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
                    {
                        _innerPageIO.WritePage(entry.Key, entry.Value);
                    }

                    _innerPageIO.Flush();

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
                        {
                            _walPageCache.Remove(entry.Key);
                            BufferPool.Return(entry.Value);
                        }

                        _wal.Truncate();
                    }

                    checkpointCompleted = true;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }
        finally
        {
            _checkpointLock.ExitWriteLock();
        }

        return checkpointCompleted;
    }

    public async Task<bool> CheckpointAsync(CancellationToken cancellationToken = default)
    {
        bool checkpointCompleted = false;
        List<KeyValuePair<int, byte[]>> pagesToWrite = null;

        // Acquire exclusive lock - waits for all active transactions to complete
        _checkpointLock.EnterWriteLock();
        try
        {
            lock (_cacheLock)
            {
                if (_walPageCache.Count > 0)
                {
                    pagesToWrite = new List<KeyValuePair<int, byte[]>>(_walPageCache);
                }
            }

            if (pagesToWrite != null)
            {
                _rwLock.EnterWriteLock();
                try
                {
                    foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await _innerPageIO.WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
                    }

                    await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
                        {
                            _walPageCache.Remove(entry.Key);
                            BufferPool.Return(entry.Value);
                        }

                        _wal.Truncate();
                    }

                    checkpointCompleted = true;
                }
                finally
                {
                    _rwLock.ExitWriteLock();
                }
            }
        }
        finally
        {
            _checkpointLock.ExitWriteLock();
        }

        return checkpointCompleted;
    }

    public void ApplyWalFrames(List<WalFrame> frames)
    {
        lock (_cacheLock)
        {
            foreach (WalFrame frame in frames)
            {
                if (frame.PageId >= 0 && frame.Data.Length > 0)
                {
                    _walPageCache[frame.PageId] = frame.Data;
                }
            }
        }
    }

    public void ClearCache()
    {
        lock (_cacheLock)
        {
            _walPageCache.Clear();
        }
    }

    private static byte DeterminePageType(byte[] pageData)
    {
        byte pageType = PageConstants.PAGE_TYPE_DOCUMENT;

        if (pageData.Length >= 2)
        {
            ushort typeMarker = BinaryHelper.ReadUInt16LE(pageData, 0);

            if (typeMarker == PageConstants.PAGE_TYPE_BTREE)
            {
                pageType = PageConstants.PAGE_TYPE_BTREE;
            }
            else if (typeMarker == PageConstants.PAGE_TYPE_DOCUMENT)
            {
                pageType = PageConstants.PAGE_TYPE_DOCUMENT;
            }
        }

        return pageType;
    }
}
