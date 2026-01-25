using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;

namespace GaldrDb.UnitTests;

internal class ConcurrencyTrackingPageIO : IPageIO
{
    private readonly InMemoryPageIO _inner;
    private readonly int _readDelayMs;
    private int _concurrentReaders;
    private int _maxConcurrentReaders;
    private readonly object _lock = new object();

    public ConcurrencyTrackingPageIO(int pageSize, int readDelayMs = 50)
    {
        _inner = new InMemoryPageIO(pageSize);
        _readDelayMs = readDelayMs;
        _concurrentReaders = 0;
        _maxConcurrentReaders = 0;
    }

    public int MaxConcurrentReaders
    {
        get { lock (_lock) { return _maxConcurrentReaders; } }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        _inner.WritePage(pageId, data);
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        lock (_lock)
        {
            _concurrentReaders++;
            if (_concurrentReaders > _maxConcurrentReaders)
            {
                _maxConcurrentReaders = _concurrentReaders;
            }
        }

        try
        {
            Thread.Sleep(_readDelayMs);
            _inner.ReadPage(pageId, destination);
        }
        finally
        {
            lock (_lock)
            {
                _concurrentReaders--;
            }
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            _concurrentReaders++;
            if (_concurrentReaders > _maxConcurrentReaders)
            {
                _maxConcurrentReaders = _concurrentReaders;
            }
        }

        try
        {
            await Task.Delay(_readDelayMs, cancellationToken).ConfigureAwait(false);
            _inner.ReadPage(pageId, destination.Span);
        }
        finally
        {
            lock (_lock)
            {
                _concurrentReaders--;
            }
        }
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        _inner.WritePage(pageId, data.Span);
        return Task.CompletedTask;
    }

    public void Flush() { }
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public void SetLength(long newSize) { }
    public void Close() { }
    public void Dispose() { _inner.Dispose(); }
}
