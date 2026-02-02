using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

/// <summary>
/// Manages page-level locks for concurrent access to document pages.
/// Uses async-compatible reader-writer locks to allow concurrent reads while serializing writes.
/// </summary>
internal class PageLockManager : IDisposable
{
    private readonly ConcurrentDictionary<int, AsyncReaderWriterLock> _pageLocks;
    private bool _disposed;

    public PageLockManager()
    {
        _pageLocks = new ConcurrentDictionary<int, AsyncReaderWriterLock>();
        _disposed = false;
    }

    public void AcquireReadLock(int pageId)
    {
        AsyncReaderWriterLock pageLock = GetOrCreateLock(pageId);
        pageLock.EnterReadLock();
    }

    public async Task AcquireReadLockAsync(int pageId, CancellationToken cancellationToken = default)
    {
        AsyncReaderWriterLock pageLock = GetOrCreateLock(pageId);
        await pageLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
    }

    public void ReleaseReadLock(int pageId)
    {
        if (_pageLocks.TryGetValue(pageId, out AsyncReaderWriterLock pageLock))
        {
            pageLock.ExitReadLock();
        }
    }

    public void AcquireWriteLock(int pageId)
    {
        AsyncReaderWriterLock pageLock = GetOrCreateLock(pageId);
        pageLock.EnterWriteLock();
    }

    public async Task AcquireWriteLockAsync(int pageId, CancellationToken cancellationToken = default)
    {
        AsyncReaderWriterLock pageLock = GetOrCreateLock(pageId);
        await pageLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
    }

    public void ReleaseWriteLock(int pageId)
    {
        if (_pageLocks.TryGetValue(pageId, out AsyncReaderWriterLock pageLock))
        {
            pageLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Acquires write locks on multiple pages in sorted order to prevent deadlocks.
    /// </summary>
    public void AcquireWriteLocks(int[] pageIds, int count)
    {
        int[] sortedIds = new int[count];
        Array.Copy(pageIds, sortedIds, count);
        Array.Sort(sortedIds);

        for (int i = 0; i < count; i++)
        {
            AcquireWriteLock(sortedIds[i]);
        }
    }

    /// <summary>
    /// Releases write locks on multiple pages.
    /// </summary>
    public void ReleaseWriteLocks(int[] pageIds, int count)
    {
        for (int i = 0; i < count; i++)
        {
            ReleaseWriteLock(pageIds[i]);
        }
    }

    /// <summary>
    /// Acquires write locks on multiple pages in sorted order to prevent deadlocks (async).
    /// </summary>
    public async Task AcquireWriteLocksAsync(int[] pageIds, int count, CancellationToken cancellationToken = default)
    {
        int[] sortedIds = new int[count];
        Array.Copy(pageIds, sortedIds, count);
        Array.Sort(sortedIds);

        for (int i = 0; i < count; i++)
        {
            await AcquireWriteLockAsync(sortedIds[i], cancellationToken).ConfigureAwait(false);
        }
    }

    private AsyncReaderWriterLock GetOrCreateLock(int pageId)
    {
        return _pageLocks.GetOrAdd(pageId, _ => new AsyncReaderWriterLock());
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            foreach (AsyncReaderWriterLock lockObj in _pageLocks.Values)
            {
                lockObj.Dispose();
            }
            _pageLocks.Clear();
        }
    }
}
