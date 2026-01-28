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

    // Per-transaction tracking of uncommitted page writes
    // Key: pageId, Value: PageWriteEntry containing the frame we wrote and what _pageLatestFrame was at write time
    private readonly AsyncLocal<Dictionary<int, PageWriteEntry>> _txPageWrites;

    // Lock for serializing WAL commits
    private readonly object _commitLock;

    // Semaphore for checkpoint (only one checkpoint at a time, but doesn't block readers/writers)
    private readonly SemaphoreSlim _checkpointSemaphore;

    // Lock for coordinating base file access during checkpoint
    private readonly object _rwLock;

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
        _currentTxId = new AsyncLocal<ulong?>();
        _txPageWrites = new AsyncLocal<Dictionary<int, PageWriteEntry>>();
        _commitLock = new object();
        _checkpointSemaphore = new SemaphoreSlim(1, 1);
        _rwLock = new object();
        _commitFramesList = new List<WalFrameEntry>();
        _commitPageIdsList = new List<int>();
        _disposed = false;
    }

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

        _currentTxId.Value = txId;
        _txPageWrites.Value = new Dictionary<int, PageWriteEntry>();
    }

    public void CommitTransaction()
    {
        ulong? txId = _currentTxId.Value;
        if (!txId.HasValue)
        {
            throw new InvalidOperationException("No transaction in progress");
        }

        Dictionary<int, PageWriteEntry> txPageWrites = _txPageWrites.Value;

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
                    walStartFrame = _wal.WriteFrameEntries(txId.Value, _commitFramesList);
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

        _currentTxId.Value = null;
        _txPageWrites.Value = null;
    }

    public void AbortTransaction()
    {
        ulong? txId = _currentTxId.Value;
        if (txId.HasValue)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = _txPageWrites.Value;

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

            _currentTxId.Value = null;
            _txPageWrites.Value = null;
        }
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        bool found = false;

        // Check current transaction's uncommitted writes first (read-your-own-writes)
        ulong? currentTxId = _currentTxId.Value;
        if (currentTxId.HasValue)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = _txPageWrites.Value;
            if (txPageWrites != null && txPageWrites.TryGetValue(pageId, out PageWriteEntry writeInfo))
            {
                lock (_cacheLock)
                {
                    if (_walFrames.TryGetValue(writeInfo.FrameNum, out WalFrameEntry entry))
                    {
                        entry.Data.AsSpan().CopyTo(destination);
                        found = true;
                    }
                }
            }
        }

        // Check for committed data in WAL
        if (!found)
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

        // Fall back to base file
        if (!found)
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

                    // Return old buffer if this page was already written in this transaction
                    Dictionary<int, PageWriteEntry> txPageWrites = _txPageWrites.Value;
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
                        // First write to this page in this transaction - record current committed frame as base
                        _pageLatestFrame.TryGetValue(pageId, out baseFrame);
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
        bool found = false;

        // Check current transaction's uncommitted writes first (read-your-own-writes)
        ulong? currentTxId = _currentTxId.Value;
        if (currentTxId.HasValue)
        {
            Dictionary<int, PageWriteEntry> txPageWrites = _txPageWrites.Value;
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

        // Check for committed data in WAL
        if (!found)
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

        // Fall back to base file
        if (!found)
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
                            if (await _wal.ReadFrameDataAsync(kvp.Value, buffer.AsMemory(), cancellationToken).ConfigureAwait(false))
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
                _checkpointSemaphore.Release();
            }
        }

        return madeProgress;
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
