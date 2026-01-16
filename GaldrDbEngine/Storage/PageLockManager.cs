using System;
using System.Collections.Concurrent;
using System.Threading;

namespace GaldrDbEngine.Storage;

/// <summary>
/// Manages page-level locks for concurrent access to document pages.
/// Uses reader-writer locks to allow concurrent reads while serializing writes.
/// </summary>
public class PageLockManager : IDisposable
{
    private readonly ConcurrentDictionary<int, ReaderWriterLockSlim> _pageLocks;
    private bool _disposed;

    public PageLockManager()
    {
        _pageLocks = new ConcurrentDictionary<int, ReaderWriterLockSlim>();
        _disposed = false;
    }

    public void AcquireReadLock(int pageId)
    {
        ReaderWriterLockSlim pageLock = GetOrCreateLock(pageId);
        pageLock.EnterReadLock();
    }

    public void ReleaseReadLock(int pageId)
    {
        if (_pageLocks.TryGetValue(pageId, out ReaderWriterLockSlim pageLock))
        {
            if (pageLock.IsReadLockHeld)
            {
                pageLock.ExitReadLock();
            }
        }
    }

    public void AcquireWriteLock(int pageId)
    {
        ReaderWriterLockSlim pageLock = GetOrCreateLock(pageId);
        pageLock.EnterWriteLock();
    }

    public void ReleaseWriteLock(int pageId)
    {
        if (_pageLocks.TryGetValue(pageId, out ReaderWriterLockSlim pageLock))
        {
            if (pageLock.IsWriteLockHeld)
            {
                pageLock.ExitWriteLock();
            }
        }
    }

    private ReaderWriterLockSlim GetOrCreateLock(int pageId)
    {
        return _pageLocks.GetOrAdd(pageId, _ => new ReaderWriterLockSlim());
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            foreach (ReaderWriterLockSlim lockSlim in _pageLocks.Values)
            {
                lockSlim.Dispose();
            }
            _pageLocks.Clear();
        }
    }
}
