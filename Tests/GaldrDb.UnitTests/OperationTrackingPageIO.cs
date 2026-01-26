using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;

namespace GaldrDb.UnitTests;

internal class OperationTrackingPageIO : IPageIO
{
    private readonly InMemoryPageIO _inner;
    private int _flushCount;
    private int _flushAsyncCount;
    private int _writeCount;
    private long _lastSetLength;
    private bool _disposed;

    public OperationTrackingPageIO(int pageSize)
    {
        _inner = new InMemoryPageIO(pageSize);
        _flushCount = 0;
        _flushAsyncCount = 0;
        _writeCount = 0;
        _lastSetLength = -1;
        _disposed = false;
    }

    public int FlushCount => _flushCount;
    public int FlushAsyncCount => _flushAsyncCount;
    public int WriteCount => _writeCount;
    public long LastSetLength => _lastSetLength;
    public bool IsDisposed => _disposed;

    public void ReadPage(int pageId, Span<byte> destination)
    {
        _inner.ReadPage(pageId, destination);
    }

    public Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        return _inner.ReadPageAsync(pageId, destination, cancellationToken);
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        Interlocked.Increment(ref _writeCount);
        _inner.WritePage(pageId, data);
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _writeCount);
        return _inner.WritePageAsync(pageId, data, cancellationToken);
    }

    public void Flush()
    {
        Interlocked.Increment(ref _flushCount);
        _inner.Flush();
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _flushAsyncCount);
        return _inner.FlushAsync(cancellationToken);
    }

    public void SetLength(long newSize)
    {
        Interlocked.Exchange(ref _lastSetLength, newSize);
        _inner.SetLength(newSize);
    }

    public void Close()
    {
        _inner.Close();
    }

    public void Dispose()
    {
        _disposed = true;
        _inner.Dispose();
    }
}
