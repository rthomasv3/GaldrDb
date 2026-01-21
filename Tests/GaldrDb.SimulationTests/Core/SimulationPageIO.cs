using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;

namespace GaldrDb.SimulationTests.Core;

public class SimulationPageIO : IPageIO
{
    private readonly int _pageSize;
    private readonly Dictionary<int, byte[]> _persistedPages;
    private readonly Dictionary<int, byte[]> _unflushedPages;
    private readonly SimulationStats _stats;
    private FaultInjector _faultInjector;
    private bool _disposed;

    public SimulationPageIO(int pageSize, SimulationStats stats)
    {
        _pageSize = pageSize;
        _persistedPages = new Dictionary<int, byte[]>();
        _unflushedPages = new Dictionary<int, byte[]>();
        _stats = stats;
        _faultInjector = null;
        _disposed = false;
    }

    public int PageSize => _pageSize;
    public int PersistedPageCount => _persistedPages.Count;
    public int UnflushedPageCount => _unflushedPages.Count;

    /// <summary>
    /// Sets the fault injector for this page IO.
    /// </summary>
    public void SetFaultInjector(FaultInjector faultInjector)
    {
        _faultInjector = faultInjector;
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SimulationPageIO));
        }

        _stats.PageReads++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckReadFault(pageId);

            if (fault == FaultType.ReadError)
            {
                throw new IOException($"Simulated read error on page {pageId}");
            }

            // For CorruptRead, we read the data first, then corrupt it
            if (fault == FaultType.CorruptRead)
            {
                ReadPageInternal(pageId, destination);
                _faultInjector.CorruptData(destination);
                return;
            }
        }

        ReadPageInternal(pageId, destination);
    }

    private void ReadPageInternal(int pageId, Span<byte> destination)
    {
        // Check unflushed first (contains most recent writes)
        if (_unflushedPages.TryGetValue(pageId, out byte[] unflushedData))
        {
            unflushedData.AsSpan().CopyTo(destination);
        }
        else if (_persistedPages.TryGetValue(pageId, out byte[] persistedData))
        {
            persistedData.AsSpan().CopyTo(destination);
        }
        else
        {
            // Page doesn't exist - return zeros
            destination.Clear();
        }
    }

    // Debug: action called on every write
    public Action<int, byte[]> OnWritePage { get; set; }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SimulationPageIO));
        }

        _stats.PageWrites++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckWriteFault(pageId);

            if (fault == FaultType.WriteError)
            {
                throw new IOException($"Simulated write error on page {pageId}");
            }

            if (fault == FaultType.PartialWrite)
            {
                // Only write part of the data
                int partialSize = _faultInjector.GetPartialWriteSize(_pageSize);
                byte[] pageData = new byte[_pageSize];
                data.Slice(0, Math.Min(partialSize, data.Length)).CopyTo(pageData);
                // Rest of page remains zeros (simulating incomplete write)
                _unflushedPages[pageId] = pageData;
                OnWritePage?.Invoke(pageId, pageData);
                return;
            }
        }

        // Normal write
        byte[] fullPageData = new byte[_pageSize];
        data.CopyTo(fullPageData);
        _unflushedPages[pageId] = fullPageData;

        OnWritePage?.Invoke(pageId, fullPageData);
    }

    public void Flush()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SimulationPageIO));
        }

        _stats.PageFlushes++;

        // Move all unflushed pages to persisted
        foreach (KeyValuePair<int, byte[]> kvp in _unflushedPages)
        {
            _persistedPages[kvp.Key] = kvp.Value;
        }
        _unflushedPages.Clear();
    }

    public void SetLength(long newSize)
    {
        // No-op for simulation - pages are created on demand
    }

    public void Close()
    {
        // Match real file behavior: closing flushes pending writes
        Flush();
        Dispose();
    }

    public Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        ReadPage(pageId, destination.Span);
        return Task.CompletedTask;
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        WritePage(pageId, data.Span);
        return Task.CompletedTask;
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _disposed = true;
    }

    /// <summary>
    /// Resets the disposed state to allow reuse after Dispose().
    /// Useful for simulation tests that reopen the database.
    /// </summary>
    public void ResetDisposed()
    {
        _disposed = false;
    }

    /// <summary>
    /// Simulates a crash by discarding all unflushed pages.
    /// Call this to simulate power loss / process crash.
    /// </summary>
    public void SimulateCrash()
    {
        _stats.CrashCount++;
        _unflushedPages.Clear();
        _disposed = false; // Allow reuse after crash
    }

    /// <summary>
    /// Gets the persisted state for inspection during testing.
    /// </summary>
    public IReadOnlyDictionary<int, byte[]> GetPersistedPages()
    {
        return _persistedPages;
    }

    /// <summary>
    /// Gets the unflushed state for inspection during testing.
    /// </summary>
    public IReadOnlyDictionary<int, byte[]> GetUnflushedPages()
    {
        return _unflushedPages;
    }
}
