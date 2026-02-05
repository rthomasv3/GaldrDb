using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Transactions;

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

    public void WritePage(int pageId, ReadOnlySpan<byte> data, TransactionContext context = null)
    {
        _inner.WritePage(pageId, data, context);
    }

    public void ReadPage(int pageId, Span<byte> destination, TransactionContext context = null)
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
            _inner.ReadPage(pageId, destination, context);
        }
        finally
        {
            lock (_lock)
            {
                _concurrentReaders--;
            }
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, TransactionContext context = null, CancellationToken cancellationToken = default)
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
            _inner.ReadPage(pageId, destination.Span, context);
        }
        finally
        {
            lock (_lock)
            {
                _concurrentReaders--;
            }
        }
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        _inner.WritePage(pageId, data.Span, context);
        return Task.CompletedTask;
    }

    public void Flush() { }
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public void SetLength(long newSize) { }
    public void Close() { }
    public void Dispose() { _inner.Dispose(); }
}
