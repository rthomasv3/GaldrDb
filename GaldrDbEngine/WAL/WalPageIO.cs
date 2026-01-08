using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WalPageIO : IPageIO
{
    private readonly IPageIO _innerPageIO;
    private readonly WriteAheadLog _wal;
    private readonly int _pageSize;
    private readonly Dictionary<int, byte[]> _walPageCache;
    private readonly object _txLock;
    
    // Transaction batching state
    private ulong _activeTxId;
    private bool _inTransaction;
    private List<PendingPageWrite> _pendingWrites;
    
    private bool _disposed;

    public WalPageIO(IPageIO innerPageIO, WriteAheadLog wal, int pageSize)
    {
        _innerPageIO = innerPageIO;
        _wal = wal;
        _pageSize = pageSize;
        _walPageCache = new Dictionary<int, byte[]>();
        _txLock = new object();
        _activeTxId = 0;
        _inTransaction = false;
        _pendingWrites = new List<PendingPageWrite>();
        _disposed = false;
    }

    public bool InTransaction
    {
        get { lock (_txLock) { return _inTransaction; } }
    }

    public void BeginTransaction(ulong txId)
    {
        lock (_txLock)
        {
            while (_inTransaction)
            {
                Monitor.Wait(_txLock);
            }

            _activeTxId = txId;
            _inTransaction = true;
            _pendingWrites.Clear();
        }
    }

    public void CommitTransaction()
    {
        lock (_txLock)
        {
            if (!_inTransaction)
            {
                throw new InvalidOperationException("No transaction in progress");
            }

            // Write all pending pages to WAL
            for (int i = 0; i < _pendingWrites.Count; i++)
            {
                PendingPageWrite write = _pendingWrites[i];
                bool isLastWrite = (i == _pendingWrites.Count - 1);
                WalFrameFlags flags = isLastWrite ? WalFrameFlags.Commit : WalFrameFlags.None;

                _wal.WriteFrame(_activeTxId, write.PageId, write.PageType, write.Data, flags);

                // Update cache with committed data
                _walPageCache[write.PageId] = write.Data;
            }

            // Ensure WAL is fsynced to disk before acknowledging commit
            _wal.Flush();

            _pendingWrites.Clear();
            _inTransaction = false;
            _activeTxId = 0;

            Monitor.Pulse(_txLock);
        }
    }

    public void AbortTransaction()
    {
        lock (_txLock)
        {
            _pendingWrites.Clear();
            _inTransaction = false;
            _activeTxId = 0;

            Monitor.Pulse(_txLock);
        }
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        bool found = false;
        byte[] foundData = null;

        lock (_txLock)
        {
            // First check pending writes from current transaction (read your own writes)
            if (_inTransaction && !found)
            {
                for (int i = _pendingWrites.Count - 1; i >= 0; i--)
                {
                    if (_pendingWrites[i].PageId == pageId)
                    {
                        foundData = _pendingWrites[i].Data;
                        found = true;
                        break;
                    }
                }
            }

            // Then check WAL cache for committed writes
            if (!found && _walPageCache.TryGetValue(pageId, out byte[] cachedPage))
            {
                foundData = cachedPage;
                found = true;
            }
        }

        if (found)
        {
            foundData.AsSpan().CopyTo(destination);
        }
        else
        {
            // Finally read from main database file
            _innerPageIO.ReadPage(pageId, destination);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        byte[] pageData = data.ToArray();
        byte pageType = DeterminePageType(pageData);

        lock (_txLock)
        {
            if (_inTransaction)
            {
                // Buffer the write for later commit
                _pendingWrites.Add(new PendingPageWrite(pageId, pageData, pageType));
            }
            else
            {
                // No active transaction - write directly with auto-commit
                // This is for non-transactional operations (e.g., initialization)
                _wal.WriteFrame(0, pageId, pageType, pageData, WalFrameFlags.Commit);
                _walPageCache[pageId] = pageData;
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
        byte[] cachedData = null;

        lock (_txLock)
        {
            // First check pending writes from current transaction
            if (_inTransaction)
            {
                for (int i = _pendingWrites.Count - 1; i >= 0; i--)
                {
                    if (_pendingWrites[i].PageId == pageId)
                    {
                        cachedData = _pendingWrites[i].Data;
                        break;
                    }
                }
            }

            // Then check WAL cache
            if (cachedData == null && _walPageCache.TryGetValue(pageId, out byte[] walCached))
            {
                cachedData = walCached;
            }
        }

        if (cachedData != null)
        {
            cachedData.AsMemory().CopyTo(destination);
        }
        else
        {
            await _innerPageIO.ReadPageAsync(pageId, destination, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        // Use sync version - the actual async work happens in CommitTransaction
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
            _innerPageIO.Dispose();
        }
    }

    public void Checkpoint()
    {
        lock (_txLock)
        {
            if (_inTransaction)
            {
                throw new InvalidOperationException("Cannot checkpoint while transaction is in progress");
            }

            // Apply all cached pages to the underlying storage
            foreach (KeyValuePair<int, byte[]> entry in _walPageCache)
            {
                _innerPageIO.WritePage(entry.Key, entry.Value);
            }

            _innerPageIO.Flush();

            // Clear the cache after checkpoint
            _walPageCache.Clear();

            // Truncate the WAL
            _wal.Truncate();
        }
    }

    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        List<KeyValuePair<int, byte[]>> pagesToWrite;

        lock (_txLock)
        {
            if (_inTransaction)
            {
                throw new InvalidOperationException("Cannot checkpoint while transaction is in progress");
            }

            // Copy pages to write outside the lock
            pagesToWrite = new List<KeyValuePair<int, byte[]>>(_walPageCache);
        }

        // Write pages outside the lock to avoid blocking other operations
        foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _innerPageIO.WritePageAsync(entry.Key, entry.Value, cancellationToken).ConfigureAwait(false);
        }

        await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);

        lock (_txLock)
        {
            // Clear only the pages we wrote (new pages might have been added)
            foreach (KeyValuePair<int, byte[]> entry in pagesToWrite)
            {
                _walPageCache.Remove(entry.Key);
            }

            // Truncate the WAL
            _wal.Truncate();
        }
    }

    public void ApplyWalFrames(List<WalFrame> frames)
    {
        lock (_txLock)
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
        lock (_txLock)
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
