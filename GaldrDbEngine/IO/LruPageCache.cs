using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.IO;

internal class LruPageCache : IPageIO
{
    private readonly IPageIO _innerPageIO;
    private readonly int _pageSize;
    private readonly Dictionary<int, LruCacheEntry> _cache;
    private readonly LinkedList<LruCacheEntry> _lruList;
    private readonly LinkedList<LruCacheEntry> _freeList;
    private readonly ReaderWriterLockSlim _cacheLock;
    private bool _disposed;

    public LruPageCache(IPageIO innerPageIO, int pageSize, int maxPages)
    {
        _innerPageIO = innerPageIO;
        _pageSize = pageSize;
        _cache = new Dictionary<int, LruCacheEntry>(maxPages);
        _lruList = new LinkedList<LruCacheEntry>();
        _freeList = new LinkedList<LruCacheEntry>();
        _cacheLock = new ReaderWriterLockSlim();
        _disposed = false;

        for (int i = 0; i < maxPages; i++)
        {
            LruCacheEntry entry = new LruCacheEntry
            {
                PageId = -1,
                Data = new byte[pageSize]
            };
            entry.Node = _freeList.AddLast(entry);
        }
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        _cacheLock.EnterUpgradeableReadLock();
        try
        {
            bool cacheHit = _cache.TryGetValue(pageId, out LruCacheEntry entry);

            if (cacheHit)
            {
                _cacheLock.EnterWriteLock();
                try
                {
                    MoveToHead(entry);
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }

                entry.Data.AsSpan(0, _pageSize).CopyTo(destination);
            }
            else
            {
                _cacheLock.ExitUpgradeableReadLock();
                _innerPageIO.ReadPage(pageId, destination);

                _cacheLock.EnterWriteLock();
                try
                {
                    if (!_cache.ContainsKey(pageId))
                    {
                        AddToCache(pageId, destination);
                    }
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }
            }
        }
        finally
        {
            if (_cacheLock.IsUpgradeableReadLockHeld)
            {
                _cacheLock.ExitUpgradeableReadLock();
            }
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        _cacheLock.EnterWriteLock();
        try
        {
            AddOrUpdateCache(pageId, data);
        }
        finally
        {
            _cacheLock.ExitWriteLock();
        }

        _innerPageIO.WritePage(pageId, data);
    }

    public void Flush()
    {
        _innerPageIO.Flush();
    }

    public void SetLength(long newSize)
    {
        _innerPageIO.SetLength(newSize);
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
            _cacheLock.Dispose();
            _innerPageIO.Dispose();
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        LruCacheEntry cachedEntry = null;
        bool cacheHit = false;

        _cacheLock.EnterUpgradeableReadLock();
        try
        {
            cacheHit = _cache.TryGetValue(pageId, out cachedEntry);

            if (cacheHit)
            {
                _cacheLock.EnterWriteLock();
                try
                {
                    MoveToHead(cachedEntry);
                }
                finally
                {
                    _cacheLock.ExitWriteLock();
                }

                cachedEntry.Data.AsSpan(0, _pageSize).CopyTo(destination.Span);
            }
        }
        finally
        {
            if (_cacheLock.IsUpgradeableReadLockHeld)
            {
                _cacheLock.ExitUpgradeableReadLock();
            }
        }

        if (!cacheHit)
        {
            await _innerPageIO.ReadPageAsync(pageId, destination, cancellationToken).ConfigureAwait(false);

            _cacheLock.EnterWriteLock();
            try
            {
                if (!_cache.ContainsKey(pageId))
                {
                    AddToCache(pageId, destination.Span);
                }
            }
            finally
            {
                _cacheLock.ExitWriteLock();
            }
        }
    }

    public async Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        _cacheLock.EnterWriteLock();
        try
        {
            AddOrUpdateCache(pageId, data.Span);
        }
        finally
        {
            _cacheLock.ExitWriteLock();
        }

        await _innerPageIO.WritePageAsync(pageId, data, cancellationToken).ConfigureAwait(false);
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _innerPageIO.FlushAsync(cancellationToken);
    }

    private void MoveToHead(LruCacheEntry entry)
    {
        _lruList.Remove(entry.Node);
        _lruList.AddFirst(entry.Node);
    }

    private void AddToCache(int pageId, ReadOnlySpan<byte> data)
    {
        LruCacheEntry entry;

        if (_freeList.Count > 0)
        {
            entry = _freeList.First.Value;
            _freeList.Remove(entry.Node);
        }
        else
        {
            entry = _lruList.Last.Value;
            _lruList.Remove(entry.Node);
            _cache.Remove(entry.PageId);
        }

        entry.PageId = pageId;
        data.CopyTo(entry.Data);
        _lruList.AddFirst(entry.Node);
        _cache[pageId] = entry;
    }

    private void AddOrUpdateCache(int pageId, ReadOnlySpan<byte> data)
    {
        if (_cache.TryGetValue(pageId, out LruCacheEntry entry))
        {
            data.CopyTo(entry.Data);
            MoveToHead(entry);
        }
        else
        {
            AddToCache(pageId, data);
        }
    }
}
