using System;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Utilities;

/// <summary>
/// Async-compatible reader-writer lock using SemaphoreSlim.
/// Allows multiple concurrent readers or a single exclusive writer.
/// </summary>
internal sealed class AsyncReaderWriterLock : IDisposable
{
    private readonly SemaphoreSlim _writeLock;
    private readonly SemaphoreSlim _readerCountLock;
    private int _readerCount;
    private bool _disposed;

    public AsyncReaderWriterLock()
    {
        _writeLock = new SemaphoreSlim(1, 1);
        _readerCountLock = new SemaphoreSlim(1, 1);
        _readerCount = 0;
        _disposed = false;
    }

    public void EnterReadLock()
    {
        _readerCountLock.Wait();
        _readerCount++;
        if (_readerCount == 1)
        {
            _writeLock.Wait();
        }
        _readerCountLock.Release();
    }

    public async Task EnterReadLockAsync(CancellationToken cancellationToken = default)
    {
        await _readerCountLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        _readerCount++;
        if (_readerCount == 1)
        {
            await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        _readerCountLock.Release();
    }

    public void ExitReadLock()
    {
        _readerCountLock.Wait();
        _readerCount--;
        if (_readerCount == 0)
        {
            _writeLock.Release();
        }
        _readerCountLock.Release();
    }

    public void EnterWriteLock()
    {
        _writeLock.Wait();
    }

    public async Task EnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public void ExitWriteLock()
    {
        _writeLock.Release();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _writeLock.Dispose();
            _readerCountLock.Dispose();
        }
    }
}
