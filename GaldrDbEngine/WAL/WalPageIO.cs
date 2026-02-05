using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Transactions;
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

    // Lock for serializing WAL commits
    private readonly object _commitLock;

    // Semaphore for checkpoint (only one checkpoint at a time, but doesn't block readers/writers)
    private readonly SemaphoreSlim _checkpointSemaphore;

    // Active snapshot tracking for checkpoint safety
    // Each entry is the _mxFrame at the time a transaction's snapshot was captured.
    // Checkpoint must not push frames beyond min(active snapshots) to the base file.
    private readonly List<long> _activeSnapshotFrames;

    // Reusable lists for CommitTransaction (protected by _commitLock)
    private readonly List<WalFrameEntry> _commitFramesList;
    private readonly List<int> _commitPageIdsList;

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
        _commitLock = new object();
        _checkpointSemaphore = new SemaphoreSlim(1, 1);
        _activeSnapshotFrames = new List<long>();
        _commitFramesList = new List<WalFrameEntry>();
        _commitPageIdsList = new List<int>();
        _disposed = false;
    }

    public TransactionContext BeginSnapshot(ulong txId)
    {
        // Check if WAL can be reset (all frames checkpointed, no uncommitted writes)
        long currentMxFrame = Interlocked.Read(ref _mxFrame);
        long currentNBackfill = Interlocked.Read(ref _nBackfill);
        long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);

        if (currentNBackfill >= currentMxFrame && currentMxFrame > 0 && currentWriteFrame == currentMxFrame)
        {
            ResetWal();
        }

        // Capture snapshot of current committed state for consistent reads
        lock (_cacheLock)
        {
            long snapshotMxFrame = Interlocked.Read(ref _mxFrame);
            _activeSnapshotFrames.Add(snapshotMxFrame);

            return new TransactionContext
            {
                TxId = txId,
                FrameSnapshot = new Dictionary<int, long>(_pageLatestFrame),
                SnapshotMxFrame = snapshotMxFrame,
                PageWrites = null
            };
        }
    }

    public void EndSnapshot(TransactionContext context)
    {
        if (context != null)
        {
            lock (_cacheLock)
            {
                _activeSnapshotFrames.Remove(context.SnapshotMxFrame);
            }
        }
    }

    /// <summary>
    /// Refreshes the transaction's snapshot to the current committed state.
    /// Used during commit retry after a PageConflictException to ensure
    /// the transaction sees a consistent view of all pages (not just the conflicted one).
    /// </summary>
    public void RefreshSnapshot(TransactionContext context)
    {
        if (context != null)
        {
            lock (_cacheLock)
            {
                // Remove old snapshot frame from active list
                _activeSnapshotFrames.Remove(context.SnapshotMxFrame);

                // Capture new snapshot
                long newSnapshotMxFrame = Interlocked.Read(ref _mxFrame);
                long oldSnapshotMxFrame = context.SnapshotMxFrame;
                context.FrameSnapshot = new Dictionary<int, long>(_pageLatestFrame);
                context.SnapshotMxFrame = newSnapshotMxFrame;

                // Add new snapshot frame to active list
                _activeSnapshotFrames.Add(newSnapshotMxFrame);
            }
        }
    }

    public void BeginWrite(TransactionContext context)
    {
        if (context != null)
        {
            context.PageWrites = new Dictionary<int, PageWriteEntry>();
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
                // Check for conflicts: did any page we wrote get committed by another transaction?
                lock (_cacheLock)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        int pageId = kvp.Key;
                        long baseFrame = kvp.Value.BaseFrame;

                        _pageLatestFrame.TryGetValue(pageId, out long currentFrame);
                        if (currentFrame != baseFrame)
                        {
                            // Another transaction committed changes to this page after we wrote it
                            throw new PageConflictException(pageId, baseFrame, currentFrame);
                        }
                    }
                }

                // Collect uncommitted frames belonging to THIS transaction
                _commitFramesList.Clear();
                _commitPageIdsList.Clear();

                lock (_cacheLock)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        int pageId = kvp.Key;
                        long frameNum = kvp.Value.FrameNum;
                        if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                        {
                            _commitFramesList.Add(entry);
                            _commitPageIdsList.Add(pageId);
                        }
                    }
                }

                long walStartFrame = 0;
                if (_commitFramesList.Count > 0)
                {
                    walStartFrame = _wal.WriteFrameEntries(context.TxId, _commitFramesList);
                    _wal.Flush();
                }

                // Release buffers and update _pageLatestFrame with actual WAL frame numbers
                lock (_cacheLock)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        long frameNum = kvp.Value.FrameNum;
                        if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                        {
                            BufferPool.Return(entry.Data);
                            _walFrames.Remove(frameNum);
                        }
                    }

                    // Update _pageLatestFrame with actual WAL frame numbers
                    for (int i = 0; i < _commitPageIdsList.Count; i++)
                    {
                        int pageId = _commitPageIdsList[i];
                        long actualWalFrame = walStartFrame + i;
                        _pageLatestFrame[pageId] = actualWalFrame;
                    }

                    // Update _writeFrameNumber to match WAL's frame counter
                    if (walStartFrame > 0)
                    {
                        long newWriteFrame = walStartFrame + _commitFramesList.Count - 1;
                        long currentWriteFrame = Interlocked.Read(ref _writeFrameNumber);
                        if (newWriteFrame > currentWriteFrame)
                        {
                            Interlocked.Exchange(ref _writeFrameNumber, newWriteFrame);
                        }
                    }
                }

                // Mark all written frames as committed (using WAL frame numbers now)
                if (walStartFrame > 0)
                {
                    long newMxFrame = walStartFrame + _commitFramesList.Count - 1;
                    long currentMxFrame = Interlocked.Read(ref _mxFrame);
                    if (newMxFrame > currentMxFrame)
                    {
                        Interlocked.Exchange(ref _mxFrame, newMxFrame);
                    }
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

            // Rollback: remove this transaction's uncommitted frames from _walFrames
            lock (_cacheLock)
            {
                if (txPageWrites != null)
                {
                    foreach (KeyValuePair<int, PageWriteEntry> kvp in txPageWrites)
                    {
                        long frameNum = kvp.Value.FrameNum;
                        if (_walFrames.TryGetValue(frameNum, out WalFrameEntry entry))
                        {
                            BufferPool.Return(entry.Data);
                            _walFrames.Remove(frameNum);
                        }
                    }
                }
            }

            context.PageWrites = null;
        }
    }

    public void ReadPage(int pageId, Span<byte> destination, TransactionContext context = null)
    {
        bool found = false;
        string readSource = null;
        long readFrame = 0;

        // Step 1: Check transaction's uncommitted writes (read-your-own-writes)
        if (context != null)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
            if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry writeInfo))
            {
                lock (_cacheLock)
                {
                    if (_walFrames.TryGetValue(writeInfo.FrameNum, out WalFrameEntry entry))
                    {
                        entry.Data.AsSpan().CopyTo(destination);
                        found = true;
                        readSource = "uncommitted";
                        readFrame = writeInfo.FrameNum;
                    }
                }
            }
        }

        // Step 2: For transactions, use snapshot for consistent reads within the transaction
        if (!found && context != null)
        {
            Dictionary<int, long> snapshot = context.FrameSnapshot;
            if (snapshot != null && snapshot.TryGetValue(pageId, out long frameNum))
            {
                if (frameNum > 0)
                {
                    found = _wal.ReadFrameData(frameNum, destination);
                }
            }
        }

        // Step 2b: During writes, check live committed state for pages not in snapshot
        if (!found && context != null)
        {
            long frameNum;
            lock (_cacheLock)
            {
                _pageLatestFrame.TryGetValue(pageId, out frameNum);
            }

            if (frameNum > 0 && frameNum > Interlocked.Read(ref _nBackfill))
            {
                found = _wal.ReadFrameData(frameNum, destination);
            }
        }

        // Step 3: No transaction - use current committed state
        if (!found && context == null)
        {
            long frameNum;
            lock (_cacheLock)
            {
                _pageLatestFrame.TryGetValue(pageId, out frameNum);
            }

            if (frameNum > 0 && frameNum > Interlocked.Read(ref _nBackfill))
            {
                found = _wal.ReadFrameData(frameNum, destination);
                if (found)
                {
                    readSource = "step3-nocontext";
                    readFrame = frameNum;
                }
            }
        }

        // Step 4: Fall back to base file
        if (!found)
        {
            _innerPageIO.ReadPage(pageId, destination);
            readSource = "basefile";
            readFrame = 0;
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data, TransactionContext context = null)
    {
        byte[] cacheData = BufferPool.Rent(_pageSize);
        bool bufferOwnershipTransferred = false;

        try
        {
            data.CopyTo(cacheData);
            byte pageType = DeterminePageType(cacheData);

            if (context != null)
            {
                // Transaction write: add to cache only, WAL write happens at commit
                lock (_cacheLock)
                {
                    long frameNum = Interlocked.Increment(ref _writeFrameNumber);

                    // Return old buffer if this page was already written in this transaction
                    Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
                    long baseFrame = 0;
                    if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry oldWriteInfo))
                    {
                        // Keep the original base frame when re-writing the same page
                        baseFrame = oldWriteInfo.BaseFrame;
                        if (_walFrames.TryGetValue(oldWriteInfo.FrameNum, out WalFrameEntry oldEntry))
                        {
                            BufferPool.Return(oldEntry.Data);
                            _walFrames.Remove(oldWriteInfo.FrameNum);
                        }
                    }
                    else
                    {
                        // First write to this page in this transaction - use snapshot frame as base
                        // so conflict detection catches pages modified after our snapshot
                        Dictionary<int, long> snapshot = context.FrameSnapshot;
                        if (snapshot != null && snapshot.TryGetValue(pageId, out long snapshotFrame))
                        {
                            baseFrame = snapshotFrame;
                        }
                        else
                        {
                            // Page not in snapshot (committed after snapshot or non-transactional write)
                            _pageLatestFrame.TryGetValue(pageId, out baseFrame);
                        }
                    }

                    _walFrames[frameNum] = new WalFrameEntry { PageId = pageId, PageType = pageType, Data = cacheData };

                    // Track this write in the per-transaction map (not the global _pageLatestFrame)
                    if (txPageWrites != null)
                    {
                        txPageWrites[pageId] = new PageWriteEntry { FrameNum = frameNum, BaseFrame = baseFrame };
                    }
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

                    if (walFrameNum > Interlocked.Read(ref _writeFrameNumber))
                    {
                        Interlocked.Exchange(ref _writeFrameNumber, walFrameNum);
                    }

                    if (walFrameNum > Interlocked.Read(ref _mxFrame))
                    {
                        Interlocked.Exchange(ref _mxFrame, walFrameNum);
                    }
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

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        bool found = false;

        // Step 1: Check transaction's uncommitted writes (read-your-own-writes)
        if (context != null)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = context.PageWrites;
            if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry writeInfo))
            {
                lock (_cacheLock)
                {
                    if (_walFrames.TryGetValue(writeInfo.FrameNum, out WalFrameEntry entry))
                    {
                        entry.Data.AsMemory().CopyTo(destination);
                        found = true;
                    }
                }
            }
        }

        // Step 2: For transactions, use snapshot for consistent reads within the transaction
        if (!found && context != null)
        {
            Dictionary<int, long> snapshot = context.FrameSnapshot;
            if (snapshot != null && snapshot.TryGetValue(pageId, out long frameNum))
            {
                if (frameNum > 0)
                {
                    found = await _wal.ReadFrameDataAsync(frameNum, destination, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        // Step 2b: During writes, check live committed state for pages not in snapshot
        if (!found && context != null)
        {
            long frameNum;
            lock (_cacheLock)
            {
                _pageLatestFrame.TryGetValue(pageId, out frameNum);
            }

            if (frameNum > 0 && frameNum > Interlocked.Read(ref _nBackfill))
            {
                found = await _wal.ReadFrameDataAsync(frameNum, destination, cancellationToken).ConfigureAwait(false);
            }
        }

        // Step 3: No transaction - use current committed state
        if (!found && context == null)
        {
            long frameNum;
            lock (_cacheLock)
            {
                _pageLatestFrame.TryGetValue(pageId, out frameNum);
            }

            if (frameNum > 0 && frameNum > Interlocked.Read(ref _nBackfill))
            {
                found = await _wal.ReadFrameDataAsync(frameNum, destination, cancellationToken).ConfigureAwait(false);
            }
        }

        // Step 4: Fall back to base file
        if (!found)
        {
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
        await _wal.FlushAsync(cancellationToken).ConfigureAwait(false);
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

            _checkpointSemaphore.Dispose();
            _innerPageIO.Dispose();
        }
    }

    public bool Checkpoint()
    {
        bool madeProgress = false;

        if (_checkpointSemaphore.Wait(0))
        {
            try
            {
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                long checkpointLimit;

                lock (_cacheLock)
                {
                    checkpointLimit = GetCheckpointLimit(currentMxFrame);
                }

                if (checkpointLimit > currentNBackfill)
                {
                    // Collect page IDs and frame numbers to checkpoint
                    List<KeyValuePair<int, long>> pagesToCheckpoint = new List<KeyValuePair<int, long>>();

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in _pageLatestFrame)
                        {
                            if (kvp.Value > currentNBackfill && kvp.Value <= checkpointLimit)
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
                            if (_wal.ReadFrameData(kvp.Value, buffer))
                            {
                                _innerPageIO.WritePage(kvp.Key, buffer);
                            }
                        }

                        _innerPageIO.Flush();
                    }
                    finally
                    {
                        BufferPool.Return(buffer);
                    }

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, checkpointLimit);

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
                _checkpointSemaphore.Release();
            }
        }

        return madeProgress;
    }

    public async Task<bool> CheckpointAsync(CancellationToken cancellationToken = default)
    {
        bool madeProgress = false;

        if (await _checkpointSemaphore.WaitAsync(0, cancellationToken).ConfigureAwait(false))
        {
            try
            {
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentNBackfill = Interlocked.Read(ref _nBackfill);
                long checkpointLimit;

                lock (_cacheLock)
                {
                    checkpointLimit = GetCheckpointLimit(currentMxFrame);
                }

                if (checkpointLimit > currentNBackfill)
                {
                    // Collect page IDs and frame numbers to checkpoint
                    List<KeyValuePair<int, long>> pagesToCheckpoint = new List<KeyValuePair<int, long>>();

                    lock (_cacheLock)
                    {
                        foreach (KeyValuePair<int, long> kvp in _pageLatestFrame)
                        {
                            if (kvp.Value > currentNBackfill && kvp.Value <= checkpointLimit)
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
                            if (await _wal.ReadFrameDataAsync(kvp.Value, buffer.AsMemory(), cancellationToken).ConfigureAwait(false))
                            {
                                await _innerPageIO.WritePageAsync(kvp.Key, buffer, null, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        await _innerPageIO.FlushAsync(cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        BufferPool.Return(buffer);
                    }

                    // Update nBackfill
                    Interlocked.Exchange(ref _nBackfill, checkpointLimit);

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
                _checkpointSemaphore.Release();
            }
        }

        return madeProgress;
    }

    /// <summary>
    /// Returns the maximum frame number that checkpoint can safely push to the base file.
    /// If active snapshots exist, caps at the minimum snapshot frame to preserve snapshot isolation.
    /// Must be called under _cacheLock.
    /// </summary>
    private long GetCheckpointLimit(long currentMxFrame)
    {
        long limit = currentMxFrame;

        if (_activeSnapshotFrames.Count > 0)
        {
            long min = long.MaxValue;
            for (int i = 0; i < _activeSnapshotFrames.Count; i++)
            {
                if (_activeSnapshotFrames[i] < min)
                {
                    min = _activeSnapshotFrames[i];
                }
            }

            if (min < limit)
            {
                limit = min;
            }
        }

        return limit;
    }

    private void ResetWal()
    {
        lock (_commitLock)
        {
            bool shouldTruncate = false;

            lock (_cacheLock)
            {
                // Re-verify under lock to prevent TOCTOU race:
                // another thread may have begun writing frames since we checked
                long currentMxFrame = Interlocked.Read(ref _mxFrame);
                long currentNBackfill = Interlocked.Read(ref _nBackfill);

                if (_walFrames.Count == 0 && _activeSnapshotFrames.Count == 0 && currentNBackfill >= currentMxFrame && currentMxFrame > 0)
                {
                    _pageLatestFrame.Clear();

                    Interlocked.Exchange(ref _mxFrame, 0);
                    Interlocked.Exchange(ref _nBackfill, 0);
                    Interlocked.Exchange(ref _writeFrameNumber, 0);

                    shouldTruncate = true;
                }
            }

            if (shouldTruncate)
            {
                _wal.Truncate();
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
