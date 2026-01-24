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

    // Frame tracking (replaces _walPageCache)
    private readonly Dictionary<long, WalFrameEntry> _walFrames;
    private readonly Dictionary<int, long> _pageLatestFrame;
    private readonly object _cacheLock;

    // Frame counters
    private long _mxFrame;           // Last committed frame number
    private long _nBackfill;         // Frames checkpointed to database
    private long _writeFrameNumber;  // Current write position (may include uncommitted)

    // Per-transaction WAL write buffers (cache writes are immediate, WAL writes are batched)
    private readonly ConcurrentDictionary<ulong, List<PendingPageWrite>> _txWalWrites;

    // Track current transaction per async flow (thread/task)
    private readonly AsyncLocal<ulong?> _currentTxId;

    // Lock for serializing WAL commits
    private readonly object _commitLock;

    // Mutex for checkpoint (only one checkpoint at a time, but doesn't block readers/writers)
    private readonly object _checkpointMutex;

    // Lock for coordinating base file access during checkpoint
    private readonly object _rwLock;

    private bool _disposed;

    public WalPageIO(IPageIO innerPageIO, WriteAheadLog wal, int pageSize)
    {
        _innerPageIO = innerPageIO;
        _wal = wal;
        _pageSize = pageSize;
        _walFrames = new Dictionary<long, WalFrameEntry>();
        _pageLatestFrame = new Dictionary<int, long>();
        _cacheLock = new object();
        _mxFrame = 0;
        _nBackfill = 0;
        _writeFrameNumber = 0;
        _txWalWrites = new ConcurrentDictionary<ulong, List<PendingPageWrite>>();
        _currentTxId = new AsyncLocal<ulong?>();
        _commitLock = new object();
        _checkpointMutex = new object();
        _rwLock = new object();
        _disposed = false;
    }

    public bool InTransaction
    {
        get { return _currentTxId.Value.HasValue; }
    }

    public long MxFrame => Interlocked.Read(ref _mxFrame);
    public long NBackfill => Interlocked.Read(ref _nBackfill);

    public void BeginTransaction(ulong txId)
    {
        // Check if WAL can be reset (all frames checkpointed, no uncommitted writes)
        long currentMxFrame = Interlocked.Read(ref _mxFrame);
        long currentNBackfill = Interlocked.Read(ref _nBackfill);
        long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);

        if (currentNBackfill >= currentMxFrame && currentMxFrame > 0 && currentWriteFrame == currentMxFrame)
        {
            ResetWal();
        }

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

            // Mark all written frames as committed
            Interlocked.Exchange(ref _mxFrame, Interlocked.Read(ref _writeFrameNumber));
        }

        // Return WAL buffers to pool
        foreach (PendingPageWrite write in walWrites)
        {
            BufferPool.Return(write.Data);
        }

        walWrites.Clear();
        _currentTxId.Value = null;
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
            // Rollback: remove uncommitted frames and restore _pageLatestFrame
            lock (_cacheLock)
            {
                HashSet<int> affectedPages = new HashSet<int>();
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);

                for (long f = currentMxFrame + 1; f <= currentWriteFrame; f++)
                {
                    if (_walFrames.TryGetValue(f, out WalFrameEntry entry))
                    {
                        affectedPages.Add(entry.PageId);
                        BufferPool.Return(entry.Data);
                        _walFrames.Remove(f);
                    }
                }

                // Restore _pageLatestFrame for affected pages
                foreach (int pageId in affectedPages)
                {
                    long latestCommittedFrame = 0;
                    for (long f = currentMxFrame; f >= 1; f--)
                    {
                        if (_walFrames.TryGetValue(f, out WalFrameEntry entry) && entry.PageId == pageId)
                        {
                            latestCommittedFrame = f;
                            break;
                        }
                    }

                    if (latestCommittedFrame > 0)
                    {
                        _pageLatestFrame[pageId] = latestCommittedFrame;
                    }
                    else
                    {
                        _pageLatestFrame.Remove(pageId);
                    }
                }

                Interlocked.Exchange(ref _writeFrameNumber, currentMxFrame);
            }

            // Return WAL buffers to pool
            foreach (PendingPageWrite write in walWrites)
            {
                BufferPool.Return(write.Data);
            }
            walWrites.Clear();
        }

        _currentTxId.Value = null;
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        bool foundInWal = false;

        lock (_cacheLock)
        {
            if (_pageLatestFrame.TryGetValue(pageId, out long frameNum))
            {
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                if (frameNum > currentNBackfill && _walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                {
                    entry.Data.AsSpan().CopyTo(destination);
                    foundInWal = true;
                }
            }
        }

        if (!foundInWal)
        {
            lock (_rwLock)
            {
                _innerPageIO.ReadPage(pageId, destination);
            }
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        byte[] cacheData = BufferPool.Rent(_pageSize);
        data.CopyTo(cacheData);
        byte pageType = DeterminePageType(cacheData);

        ulong? txId = _currentTxId.Value;
        if (txId.HasValue && _txWalWrites.TryGetValue(txId.Value, out List<PendingPageWrite> walWrites))
        {
            // Transaction write: add to cache and buffer for WAL
            lock (_cacheLock)
            {
                long frameNum = Interlocked.Increment(ref _writeFrameNumber);

                // Return old buffer if this page was already in cache at a different frame
                if (_pageLatestFrame.TryGetValue(pageId, out long oldFrameNum))
                {
                    if (_walFrames.TryGetValue(oldFrameNum, out WalFrameEntry oldEntry))
                    {
                        // Only return buffer if it's from an uncommitted frame we're replacing
                        long currentMxFrame = Interlocked.Read(ref _mxFrame);
                        if (oldFrameNum > currentMxFrame)
                        {
                            BufferPool.Return(oldEntry.Data);
                            _walFrames.Remove(oldFrameNum);
                        }
                    }
                }

                _walFrames[frameNum] = new WalFrameEntry { PageId = pageId, Data = cacheData };
                _pageLatestFrame[pageId] = frameNum;
            }

            // Make a separate copy for WAL buffer
            byte[] walData = BufferPool.Rent(_pageSize);
            data.CopyTo(walData);
            walWrites.Add(new PendingPageWrite(pageId, walData, pageType));
        }
        else
        {
            // Non-transaction write: auto-commit immediately
            lock (_cacheLock)
            {
                long frameNum = Interlocked.Increment(ref _writeFrameNumber);

                _walFrames[frameNum] = new WalFrameEntry { PageId = pageId, Data = cacheData };
                _pageLatestFrame[pageId] = frameNum;
            }

            lock (_commitLock)
            {
                _wal.WriteFrame(0, pageId, pageType, cacheData, WalFrameFlags.Commit);
                Interlocked.Exchange(ref _mxFrame, Interlocked.Read(ref _writeFrameNumber));
            }
        }
    }

    public void Flush()
    {
        _wal.Flush();
        _innerPageIO.Flush();
    }

    public void SetLength(long newSize)
    {
        _innerPageIO.SetLength(newSize);
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        bool foundInWal = false;

        lock (_cacheLock)
        {
            if (_pageLatestFrame.TryGetValue(pageId, out long frameNum))
            {
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                if (frameNum > currentNBackfill && _walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                {
                    entry.Data.AsMemory().CopyTo(destination);
                    foundInWal = true;
                }
            }
        }

        if (!foundInWal)
        {
            await _innerPageIO.ReadPageAsync(pageId, destination, cancellationToken).ConfigureAwait(false);
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

            // Return all cached buffers
            lock (_cacheLock)
            {
                foreach (KeyValuePair<long, WalFrameEntry> kvp in _walFrames)
                {
                    BufferPool.Return(kvp.Value.Data);
                }
                _walFrames.Clear();
                _pageLatestFrame.Clear();
            }

            _innerPageIO.Dispose();
        }
    }

    public bool Checkpoint()
    {
        bool madeProgress = false;

        if (Monitor.TryEnter(_checkpointMutex))
        {
            try
            {
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentNBackfill = Interlocked.Read(ref _nBackfill);

                if (currentMxFrame > currentNBackfill)
                {
                    // Collect frames to checkpoint
                    List<KeyValuePair<long, WalFrameEntry>> framesToCheckpoint = new List<KeyValuePair<long, WalFrameEntry>>();

                    lock (_cacheLock)
                    {
                        for (long frameNum = currentNBackfill + 1; frameNum <= currentMxFrame; frameNum++)
                        {
                            if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                            {
                                framesToCheckpoint.Add(new KeyValuePair<long, WalFrameEntry>(frameNum, entry));
                            }
                        }
                    }

                    // Write to database (outside cache lock)
                    lock (_rwLock)
                    {
                        foreach (KeyValuePair<long, WalFrameEntry> kvp in framesToCheckpoint)
                        {
                            _innerPageIO.WritePage(kvp.Value.PageId, kvp.Value.Data);
                        }

                        _innerPageIO.Flush();
                    }

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, currentMxFrame);

                    // Clear checkpointed frames from cache
                    ClearCheckpointedFrames(currentNBackfill, currentMxFrame);

                    madeProgress = true;
                }
            }
            finally
            {
                Monitor.Exit(_checkpointMutex);
            }
        }

        return madeProgress;
    }

    public async Task<bool> CheckpointAsync(CancellationToken cancellationToken = default)
    {
        bool madeProgress = false;

        if (Monitor.TryEnter(_checkpointMutex))
        {
            try
            {
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentNBackfill = Interlocked.Read(ref _nBackfill);

                if (currentMxFrame > currentNBackfill)
                {
                    // Collect frames to checkpoint
                    List<KeyValuePair<long, WalFrameEntry>> framesToCheckpoint = new List<KeyValuePair<long, WalFrameEntry>>();

                    lock (_cacheLock)
                    {
                        for (long frameNum = currentNBackfill + 1; frameNum <= currentMxFrame; frameNum++)
                        {
                            if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                            {
                                framesToCheckpoint.Add(new KeyValuePair<long, WalFrameEntry>(frameNum, entry));
                            }
                        }
                    }

                    // Write to database (outside cache lock)
                    foreach (KeyValuePair<long, WalFrameEntry> kvp in framesToCheckpoint)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await _innerPageIO.WritePageAsync(kvp.Value.PageId, kvp.Value.Data, cancellationToken).ConfigureAwait(false);
                    }

                    await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, currentMxFrame);

                    // Clear checkpointed frames from cache
                    ClearCheckpointedFrames(currentNBackfill, currentMxFrame);

                    madeProgress = true;
                }
            }
            finally
            {
                Monitor.Exit(_checkpointMutex);
            }
        }

        return madeProgress;
    }

    public void ApplyWalFrames(List<WalFrame> frames)
    {
        lock (_cacheLock)
        {
            long lastCommitFrame = 0;

            foreach (WalFrame frame in frames)
            {
                if (frame.PageId >= 0 && frame.Data.Length > 0)
                {
                    long frameNum = frame.FrameNumber;

                    // Copy data to owned buffer
                    byte[] data = BufferPool.Rent(_pageSize);
                    Array.Copy(frame.Data, 0, data, 0, Math.Min(frame.Data.Length, _pageSize));

                    _walFrames[frameNum] = new WalFrameEntry { PageId = frame.PageId, Data = data };
                    _pageLatestFrame[frame.PageId] = frameNum;

                    if (frame.IsCommit())
                    {
                        lastCommitFrame = frameNum;
                    }
                }
            }

            // Set counters based on recovered frames
            if (frames.Count > 0)
            {
                long maxFrameNum = frames[frames.Count - 1].FrameNumber;
                Interlocked.Exchange(ref _writeFrameNumber, lastCommitFrame);
                Interlocked.Exchange(ref _mxFrame, lastCommitFrame);
                Interlocked.Exchange(ref _nBackfill, 0); // Nothing checkpointed after recovery
            }
        }
    }

    public void ClearCache()
    {
        lock (_cacheLock)
        {
            foreach (KeyValuePair<long, WalFrameEntry> kvp in _walFrames)
            {
                BufferPool.Return(kvp.Value.Data);
            }
            _walFrames.Clear();
            _pageLatestFrame.Clear();
            Interlocked.Exchange(ref _mxFrame, 0);
            Interlocked.Exchange(ref _nBackfill, 0);
            Interlocked.Exchange(ref _writeFrameNumber, 0);
        }
    }

    private void ResetWal()
    {
        lock (_cacheLock)
        {
            // Clear any remaining frames (should be empty after checkpoint)
            foreach (KeyValuePair<long, WalFrameEntry> kvp in _walFrames)
            {
                BufferPool.Return(kvp.Value.Data);
            }
            _walFrames.Clear();
            _pageLatestFrame.Clear();

            // Reset counters
            Interlocked.Exchange(ref _mxFrame, 0);
            Interlocked.Exchange(ref _nBackfill, 0);
            Interlocked.Exchange(ref _writeFrameNumber, 0);
        }

        // Reset WAL file with new salts
        _wal.Truncate();
    }

    private void ClearCheckpointedFrames(long fromFrame, long toFrame)
    {
        lock (_cacheLock)
        {
            for (long frameNum = fromFrame + 1; frameNum <= toFrame; frameNum++)
            {
                if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                {
                    // Only remove from _pageLatestFrame if this was the latest frame for this page
                    if (_pageLatestFrame.TryGetValue(entry.PageId, out long latestFrame) && latestFrame == frameNum)
                    {
                        _pageLatestFrame.Remove(entry.PageId);
                    }

                    BufferPool.Return(entry.Data);
                    _walFrames.Remove(frameNum);
                }
            }
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
