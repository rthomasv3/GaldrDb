using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.IO;

internal struct BufferedFrameEntry
{
    public int PageId;
    public byte[] Data;
}

internal class BufferedPageIO : IWriteStrategy
{
    private readonly IPageIO _innerPageIO;
    private readonly int _pageSize;

    // Buffered frames keyed by WriteId (uncommitted page buffers)
    private readonly Dictionary<long, BufferedFrameEntry> _bufferedFrames;

    // Committed version per page (for conflict detection)
    private ImmutableDictionary<int, long> _pageVersions;

    // Monotonic counters
    private long _writeCounter;
    private long _versionCounter;

    // Lock ordering: always _commitLock → _bufferLock
    private readonly object _commitLock;
    private readonly object _bufferLock;

    private bool _disposed;

    public BufferedPageIO(IPageIO innerPageIO, int pageSize)
    {
        _innerPageIO = innerPageIO;
        _pageSize = pageSize;
        _bufferedFrames = new Dictionary<long, BufferedFrameEntry>();
        _pageVersions = ImmutableDictionary<int, long>.Empty;
        _writeCounter = 0;
        _versionCounter = 0;
        _commitLock = new object();
        _bufferLock = new object();
        _disposed = false;
    }

    public TransactionContext BeginSnapshot(ulong txId, ulong snapshotTxId, ulong snapshotCSN)
    {
        lock (_bufferLock)
        {
            return new TransactionContext
            {
                TxId = txId,
                SnapshotTxId = snapshotTxId,
                SnapshotCSN = snapshotCSN,
                PageVersionSnapshot = _pageVersions,
                SnapshotVersion = Interlocked.Read(ref _versionCounter),
                PageWrites = null,
                AllocatedDataPages = new List<int>(),
            };
        }
    }

    public void EndSnapshot(TransactionContext context)
    {
        // No-op for buffered IO (no WAL file tracking needed)
    }

    public void RefreshSnapshot(TransactionContext context)
    {
        if (context != null)
        {
            lock (_bufferLock)
            {
                context.PageVersionSnapshot = _pageVersions;
                context.SnapshotVersion = Interlocked.Read(ref _versionCounter);
            }

            // Clear live page reads since the snapshot has been refreshed
            context.LivePageReads?.Clear();
        }
    }

    public void BeginWrite(TransactionContext context)
    {
        if (context != null)
        {
            context.PageWrites = new Dictionary<int, PageWriteEntry>();
            context.LivePageReads = new Dictionary<int, long>();
        }
    }

    public void CommitWrite(TransactionContext context)
    {
        if (context == null)
        {
            throw new InvalidOperationException("No transaction context provided");
        }

        Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;

        lock (_commitLock)
        {
            if (txPageWrites != null && txPageWrites.Count > 0)
            {
                // Step 1: Conflict check
                lock (_bufferLock)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        int pageId = kvp.Key;
                        long baseVersion = kvp.Value.BaseVersion;

                        _pageVersions.TryGetValue(pageId, out long currentVersion);
                        if (currentVersion != baseVersion)
                        {
                            throw new PageConflictException(pageId, baseVersion, currentVersion);
                        }
                    }
                }

                // Step 2: Flush all buffered pages to inner IO
                lock (_bufferLock)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        long writeId = kvp.Value.WriteId;
                        if (_bufferedFrames.TryGetValue(writeId, out BufferedFrameEntry entry))
                        {
                            _innerPageIO.WritePage(entry.PageId, entry.Data);
                        }
                    }
                }

                // Step 3: Flush to disk
                _innerPageIO.Flush();

                // Step 4: Update page versions and clean up buffers
                lock (_bufferLock)
                {
                    ImmutableDictionary<int, long>.Builder builder = _pageVersions.ToBuilder();

                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        int pageId = kvp.Key;
                        long writeId = kvp.Value.WriteId;

                        long newVersion = Interlocked.Increment(ref _versionCounter);
                        builder[pageId] = newVersion;

                        if (_bufferedFrames.TryGetValue(writeId, out BufferedFrameEntry entry))
                        {
                            BufferPool.Return(entry.Data);
                            _bufferedFrames.Remove(writeId);
                        }
                    }

                    _pageVersions = builder.ToImmutable();
                }
            }
        }

        context.PageWrites = null;
    }

    public void AbortWrite(TransactionContext context)
    {
        if (context != null)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;

            lock (_bufferLock)
            {
                if (txPageWrites != null)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        long writeId = kvp.Value.WriteId;
                        if (_bufferedFrames.TryGetValue(writeId, out BufferedFrameEntry entry))
                        {
                            BufferPool.Return(entry.Data);
                            _bufferedFrames.Remove(writeId);
                        }
                    }
                }
            }

            context.PageWrites = null;
            context.LivePageReads = null;
        }
    }

    public void ReadPage(int pageId, Span<byte> destination, TransactionContext context = null)
    {
        bool found = false;

        // Step 1: Check transaction's uncommitted writes (read-your-own-writes)
        if (context != null)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
            if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry writeInfo))
            {
                lock (_bufferLock)
                {
                    if (_bufferedFrames.TryGetValue(writeInfo.WriteId, out BufferedFrameEntry entry))
                    {
                        entry.Data.AsSpan().CopyTo(destination);
                        found = true;
                    }
                }
            }
        }

        // Step 2: Fall back to inner IO
        if (!found)
        {
            // Record the page version at read time for conflict detection.
            // This prevents a TOCTOU race where _pageVersions changes between
            // ReadPage and WritePage, causing WritePage to use a stale baseVersion.
            if (context != null && context.LivePageReads != null && !context.LivePageReads.ContainsKey(pageId))
            {
                _pageVersions.TryGetValue(pageId, out long currentVersion);
                context.LivePageReads[pageId] = currentVersion;
            }

            _innerPageIO.ReadPage(pageId, destination);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data, TransactionContext context = null)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        bool bufferOwnershipTransferred = false;

        try
        {
            data.CopyTo(buffer);

            if (context != null)
            {
                // Transaction write: buffer for later commit
                lock (_bufferLock)
                {
                    long writeId = Interlocked.Increment(ref _writeCounter);

                    Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
                    long baseVersion = 0;

                    if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry oldWriteInfo))
                    {
                        // Keep the original base version when re-writing the same page
                        baseVersion = oldWriteInfo.BaseVersion;
                        if (_bufferedFrames.TryGetValue(oldWriteInfo.WriteId, out BufferedFrameEntry oldEntry))
                        {
                            BufferPool.Return(oldEntry.Data);
                            _bufferedFrames.Remove(oldWriteInfo.WriteId);
                        }
                    }
                    else
                    {
                        // First write to this page in this transaction - use snapshot version as base
                        ImmutableDictionary<int, long> snapshot = context.PageVersionSnapshot;
                        if (snapshot != null && snapshot.TryGetValue(pageId, out long snapshotVersion))
                        {
                            baseVersion = snapshotVersion;
                        }
                        else if (context.LivePageReads != null && context.LivePageReads.TryGetValue(pageId, out long liveReadVersion))
                        {
                            // Use the version that was recorded when this page was actually read.
                            // This prevents a TOCTOU race where _pageVersions changes between
                            // ReadPage and WritePage due to a concurrent commit.
                            baseVersion = liveReadVersion;
                        }
                        else
                        {
                            _pageVersions.TryGetValue(pageId, out baseVersion);
                        }
                    }

                    _bufferedFrames[writeId] = new BufferedFrameEntry { PageId = pageId, Data = buffer };

                    if (txPageWrites != null)
                    {
                        txPageWrites[pageId] = new PageWriteEntry { WriteId = writeId, BaseVersion = baseVersion };
                    }

                    bufferOwnershipTransferred = true;
                }
            }
            else
            {
                // Non-transaction write: write-through directly to inner IO.
                // Do not update version counters — null-context writes are advisory
                // (e.g., FSM updates, initialization) and should not interfere with
                // transaction conflict detection.
                _innerPageIO.WritePage(pageId, data);
            }
        }
        finally
        {
            if (!bufferOwnershipTransferred)
            {
                BufferPool.Return(buffer);
            }
        }
    }

    public void Flush()
    {
        _innerPageIO.Flush();
    }

    public void SetLength(long newSize)
    {
        _innerPageIO.SetLength(newSize);
    }

    public void Close()
    {
        _innerPageIO.Close();
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        bool found = false;

        // Step 1: Check transaction's uncommitted writes (read-your-own-writes)
        if (context != null)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
            if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry writeInfo))
            {
                lock (_bufferLock)
                {
                    if (_bufferedFrames.TryGetValue(writeInfo.WriteId, out BufferedFrameEntry entry))
                    {
                        entry.Data.AsMemory().CopyTo(destination);
                        found = true;
                    }
                }
            }
        }

        // Step 2: Fall back to inner IO
        if (!found)
        {
            // Record the page version at read time for conflict detection.
            if (context != null && context.LivePageReads != null && !context.LivePageReads.ContainsKey(pageId))
            {
                _pageVersions.TryGetValue(pageId, out long currentVersion);
                context.LivePageReads[pageId] = currentVersion;
            }

            await _innerPageIO.ReadPageAsync(pageId, destination, context, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        WritePage(pageId, data.Span, context);
        return Task.CompletedTask;
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public bool Checkpoint()
    {
        return false;
    }

    public Task<bool> CheckpointAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(false);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            // Return all buffered data
            lock (_bufferLock)
            {
                foreach (KeyValuePair<long, BufferedFrameEntry> kvp in _bufferedFrames)
                {
                    BufferPool.Return(kvp.Value.Data);
                }
                _bufferedFrames.Clear();
                _pageVersions = ImmutableDictionary<int, long>.Empty;
            }

            _innerPageIO.Dispose();
        }
    }
}
