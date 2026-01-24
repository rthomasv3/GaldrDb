using System;
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

    // Track current transaction per async flow (thread/task)
    private readonly AsyncLocal<ulong?> _currentTxId;

    // Track previous _pageLatestFrame values for rollback (pageId -> previous frame, null means no previous)
    private readonly Dictionary<int, long> _txPreviousFrames;

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
        _currentTxId = new AsyncLocal<ulong?>();
        _txPreviousFrames = new Dictionary<int, long>();
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

        lock (_cacheLock)
        {
            _txPreviousFrames.Clear();
        }

        _currentTxId.Value = txId;
    }

    public void CommitTransaction()
    {
        ulong? txId = _currentTxId.Value;
        if (!txId.HasValue)
        {
            throw new InvalidOperationException("No transaction in progress");
        }

        lock (_commitLock)
        {
            long currentMxFrame = Interlocked.Read(ref _mxFrame);
            long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);

            if (currentWriteFrame > currentMxFrame)
            {
                // Collect uncommitted frames from _walFrames, preserving pageId order
                List<WalFrameEntry> framesToWrite = new List<WalFrameEntry>();
                List<int> pageIdsInOrder = new List<int>();
                lock (_cacheLock)
                {
                    for (long frameNum = currentMxFrame + 1; frameNum <= currentWriteFrame; frameNum++)
                    {
                        if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                        {
                            framesToWrite.Add(entry);
                            pageIdsInOrder.Add(entry.PageId);
                        }
                    }
                }

                long walStartFrame = 0;
                if (framesToWrite.Count > 0)
                {
                    walStartFrame = _wal.WriteFrameEntries(txId.Value, framesToWrite);
                    _wal.Flush();
                }

                // Release buffers and update _pageLatestFrame with actual WAL frame numbers
                lock (_cacheLock)
                {
                    for (long frameNum = currentMxFrame + 1; frameNum <= currentWriteFrame; frameNum++)
                    {
                        if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                        {
                            BufferPool.Return(entry.Data);
                            _walFrames.Remove(frameNum);
                        }
                    }

                    // Update _pageLatestFrame with actual WAL frame numbers
                    for (int i = 0; i < pageIdsInOrder.Count; i++)
                    {
                        int pageId = pageIdsInOrder[i];
                        long actualWalFrame = walStartFrame + i;
                        _pageLatestFrame[pageId] = actualWalFrame;
                    }

                    // Update _writeFrameNumber to match WAL's frame counter
                    if (walStartFrame > 0)
                    {
                        Interlocked.Exchange(ref _writeFrameNumber, walStartFrame + framesToWrite.Count - 1);
                    }
                }

                // Mark all written frames as committed (using WAL frame numbers now)
                long newMxFrame = walStartFrame > 0 ? walStartFrame + framesToWrite.Count - 1 : currentMxFrame;
                Interlocked.Exchange(ref _mxFrame, newMxFrame);
            }
        }

        lock (_cacheLock)
        {
            _txPreviousFrames.Clear();
        }

        _currentTxId.Value = null;
    }

    public void AbortTransaction()
    {
        if (_currentTxId.Value.HasValue)
        {
            // Rollback: remove uncommitted frames and restore _pageLatestFrame
            lock (_cacheLock)
            {
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);

                // Remove uncommitted frames and return buffers
                for (long f = currentMxFrame + 1; f <= currentWriteFrame; f++)
                {
                    if (_walFrames.TryGetValue(f, out WalFrameEntry entry))
                    {
                        BufferPool.Return(entry.Data);
                        _walFrames.Remove(f);
                    }
                }

                // Restore _pageLatestFrame from saved previous values
                foreach (KeyValuePair<int, long> kvp in _txPreviousFrames)
                {
                    if (kvp.Value > 0)
                    {
                        _pageLatestFrame[kvp.Key] = kvp.Value;
                    }
                    else
                    {
                        _pageLatestFrame.Remove(kvp.Key);
                    }
                }

                _txPreviousFrames.Clear();
                Interlocked.Exchange(ref _writeFrameNumber, currentMxFrame);
            }

            _currentTxId.Value = null;
        }
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        bool foundInWal = false;
        long frameNum = 0;
        bool isUncommitted = false;

        lock (_cacheLock)
        {
            if (_pageLatestFrame.TryGetValue(pageId, out frameNum))
            {
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                long currentMxFrame = Interlocked.Read(ref _mxFrame);

                if (frameNum > currentNBackfill)
                {
                    // Check if uncommitted (still in _walFrames with buffer)
                    if (frameNum > currentMxFrame && _walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                    {
                        entry.Data.AsSpan().CopyTo(destination);
                        foundInWal = true;
                        isUncommitted = true;
                    }
                }
            }
        }

        if (!foundInWal && frameNum > 0 && !isUncommitted)
        {
            // Read committed frame from WAL file
            long currentNBackfill = Interlocked.Read(ref _nBackfill);
            if (frameNum > currentNBackfill)
            {
                foundInWal = _wal.ReadFrameData(frameNum, destination);
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
        bool bufferOwnershipTransferred = false;

        try
        {
            data.CopyTo(cacheData);
            byte pageType = DeterminePageType(cacheData);

            ulong? txId = _currentTxId.Value;
            if (txId.HasValue)
            {
                // Transaction write: add to cache only, WAL write happens at commit
                lock (_cacheLock)
                {
                    long frameNum = Interlocked.Increment(ref _writeFrameNumber);

                    // Save previous frame for potential rollback (only first write to this page in transaction)
                    if (!_txPreviousFrames.ContainsKey(pageId))
                    {
                        if (_pageLatestFrame.TryGetValue(pageId, out long prevFrame))
                        {
                            _txPreviousFrames[pageId] = prevFrame;
                        }
                        else
                        {
                            _txPreviousFrames[pageId] = 0; // 0 means no previous frame
                        }
                    }

                    // Return old buffer if this page was already written in this transaction
                    // (All frames in _walFrames are uncommitted, so we can always clean up)
                    if (_pageLatestFrame.TryGetValue(pageId, out long oldFrameNum))
                    {
                        if (_walFrames.TryGetValue(oldFrameNum, out WalFrameEntry oldEntry))
                        {
                            BufferPool.Return(oldEntry.Data);
                            _walFrames.Remove(oldFrameNum);
                        }
                    }

                    _walFrames[frameNum] = new WalFrameEntry { PageId = pageId, PageType = pageType, Data = cacheData };
                    _pageLatestFrame[pageId] = frameNum;
                    bufferOwnershipTransferred = true;
                }
            }
            else
            {
                // Non-transaction write: write to WAL immediately and release buffer
                long walFrameNum;
                lock (_commitLock)
                {
                    walFrameNum = _wal.WriteFrame(0, pageId, pageType, cacheData, WalFrameFlags.Commit);
                }

                lock (_cacheLock)
                {
                    _pageLatestFrame[pageId] = walFrameNum;
                    Interlocked.Exchange(ref _writeFrameNumber, walFrameNum);
                    Interlocked.Exchange(ref _mxFrame, walFrameNum);
                }
            }
        }
        finally
        {
            // Return buffer if ownership wasn't transferred to _walFrames
            if (!bufferOwnershipTransferred)
            {
                BufferPool.Return(cacheData);
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
        long frameNum = 0;
        bool isUncommitted = false;

        lock (_cacheLock)
        {
            if (_pageLatestFrame.TryGetValue(pageId, out frameNum))
            {
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                long currentMxFrame = Interlocked.Read(ref _mxFrame);

                if (frameNum > currentNBackfill)
                {
                    // Check if uncommitted (still in _walFrames with buffer)
                    if (frameNum > currentMxFrame && _walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                    {
                        entry.Data.AsMemory().CopyTo(destination);
                        foundInWal = true;
                        isUncommitted = true;
                    }
                }
            }
        }

        if (!foundInWal && frameNum > 0 && !isUncommitted)
        {
            // Read committed frame from WAL file
            long currentNBackfill = Interlocked.Read(ref _nBackfill);
            if (frameNum > currentNBackfill)
            {
                foundInWal = _wal.ReadFrameData(frameNum, destination.Span);
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
                    // Collect page IDs and frame numbers to checkpoint
                    List<KeyValuePair<int, long>> pagesToCheckpoint = new List<KeyValuePair<int, long>>();

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in _pageLatestFrame)
                        {
                            if (kvp.Value > currentNBackfill && kvp.Value <= currentMxFrame)
                            {
                                pagesToCheckpoint.Add(kvp);
                            }
                        }
                    }

                    // Read from WAL file and write to database
                    byte[] buffer = BufferPool.Rent(_pageSize);
                    try
                    {
                        lock (_rwLock)
                        {
                            foreach (KeyValuePair<int, long> kvp in pagesToCheckpoint)
                            {
                                if (_wal.ReadFrameData(kvp.Value, buffer))
                                {
                                    _innerPageIO.WritePage(kvp.Key, buffer);
                                }
                            }

                            _innerPageIO.Flush();
                        }
                    }
                    finally
                    {
                        BufferPool.Return(buffer);
                    }

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, currentMxFrame);

                    // Clear checkpointed entries from _pageLatestFrame
                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in pagesToCheckpoint)
                        {
                            if (_pageLatestFrame.TryGetValue(kvp.Key, out long latestFrame) && latestFrame == kvp.Value)
                            {
                                _pageLatestFrame.Remove(kvp.Key);
                            }
                        }
                    }

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
                    // Collect page IDs and frame numbers to checkpoint
                    List<KeyValuePair<int, long>> pagesToCheckpoint = new List<KeyValuePair<int, long>>();

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in _pageLatestFrame)
                        {
                            if (kvp.Value > currentNBackfill && kvp.Value <= currentMxFrame)
                            {
                                pagesToCheckpoint.Add(kvp);
                            }
                        }
                    }

                    // Read from WAL file and write to database
                    byte[] buffer = BufferPool.Rent(_pageSize);
                    try
                    {
                        foreach (KeyValuePair<int, long> kvp in pagesToCheckpoint)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            if (_wal.ReadFrameData(kvp.Value, buffer))
                            {
                                await _innerPageIO.WritePageAsync(kvp.Key, buffer, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        BufferPool.Return(buffer);
                    }

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, currentMxFrame);

                    // Clear checkpointed entries from _pageLatestFrame
                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in pagesToCheckpoint)
                        {
                            if (_pageLatestFrame.TryGetValue(kvp.Key, out long latestFrame) && latestFrame == kvp.Value)
                            {
                                _pageLatestFrame.Remove(kvp.Key);
                            }
                        }
                    }

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

                    // Just track the frame in _pageLatestFrame - data stays in WAL file
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
