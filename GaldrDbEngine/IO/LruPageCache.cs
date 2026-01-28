using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.IO;

/// <summary>
/// Page cache using approximate LRU eviction with sampled selection.
/// Uses ConcurrentDictionary for lock-free reads and sampled eviction
/// to avoid the synchronization overhead of perfect LRU tracking.
/// </summary>
internal class LruPageCache : IPageIO
{
    private readonly IPageIO _innerPageIO;
    private readonly int _pageSize;
    private readonly ConcurrentDictionary<int, LruCacheEntry> _cache;
    private readonly int _maxEntries;
    private readonly int _evictionSampleSize;
    private readonly object _evictionLock;
    private bool _disposed;

    private const int DEFAULT_SAMPLE_SIZE = 5;

    public LruPageCache(IPageIO innerPageIO, int pageSize, int maxPages)
    {
        _innerPageIO = innerPageIO;
        _pageSize = pageSize;
        _cache = new ConcurrentDictionary<int, LruCacheEntry>();
        _maxEntries = maxPages;
        _evictionSampleSize = Math.Min(DEFAULT_SAMPLE_SIZE, maxPages);
        _evictionLock = new object();
        _disposed = false;
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        bool cacheHit = _cache.TryGetValue(pageId, out LruCacheEntry entry);

        if (cacheHit)
        {
            while (true)
            {
                int v1 = entry.ReadVersion();
                if (entry.IsWriteInProgress(v1))
                {
                    continue;
                }
                entry.Data.AsSpan(0, _pageSize).CopyTo(destination);
                int v2 = entry.ReadVersion();
                if (v1 == v2)
                {
                    break;
                }
            }
            entry.Touch();
        }
        else
        {
            _innerPageIO.ReadPage(pageId, destination);
            AddToCache(pageId, destination);
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        bool cacheHit = _cache.TryGetValue(pageId, out LruCacheEntry entry);

        if (cacheHit)
        {
            while (true)
            {
                int v1 = entry.ReadVersion();
                if (entry.IsWriteInProgress(v1))
                {
                    continue;
                }
                entry.Data.AsSpan(0, _pageSize).CopyTo(destination.Span);
                int v2 = entry.ReadVersion();
                if (v1 == v2)
                {
                    break;
                }
            }
            entry.Touch();
        }
        else
        {
            await _innerPageIO.ReadPageAsync(pageId, destination, cancellationToken).ConfigureAwait(false);
            AddToCache(pageId, destination.Span);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        AddOrUpdateCache(pageId, data);
        _innerPageIO.WritePage(pageId, data);
    }

    public async Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        AddOrUpdateCache(pageId, data.Span);
        await _innerPageIO.WritePageAsync(pageId, data, cancellationToken).ConfigureAwait(false);
    }

    public void Flush()
    {
        _innerPageIO.Flush();
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _innerPageIO.FlushAsync(cancellationToken);
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
            _cache.Clear();
            _innerPageIO.Dispose();
        }
    }

    private void AddToCache(int pageId, ReadOnlySpan<byte> data)
    {
        if (!_cache.ContainsKey(pageId))
        {
            EnsureCapacity();

            LruCacheEntry newEntry = new LruCacheEntry(pageId, _pageSize);
            data.CopyTo(newEntry.Data);

            _cache.TryAdd(pageId, newEntry);
        }
    }

    private void AddOrUpdateCache(int pageId, ReadOnlySpan<byte> data)
    {
        while (true)
        {
            bool exists = _cache.TryGetValue(pageId, out LruCacheEntry entry);

            if (exists)
            {
                lock (entry.WriteLock)
                {
                    entry.BeginWrite();
                    data.CopyTo(entry.Data);
                    entry.EndWrite();
                }
                entry.Touch();
                break;
            }
            else
            {
                EnsureCapacity();

                LruCacheEntry newEntry = new LruCacheEntry(pageId, _pageSize);
                data.CopyTo(newEntry.Data);

                if (_cache.TryAdd(pageId, newEntry))
                {
                    break;
                }
            }
        }
    }

    private void EnsureCapacity()
    {
        if (_cache.Count >= _maxEntries)
        {
            lock (_evictionLock)
            {
                while (_cache.Count >= _maxEntries)
                {
                    EvictOne();
                }
            }
        }
    }

    private void EvictOne()
    {
        List<int> keys = _cache.Keys.ToList();

        if (keys.Count > 0)
        {
            int oldestKey = -1;
            long oldestTicks = long.MaxValue;

            int sampleCount = Math.Min(_evictionSampleSize, keys.Count);
            for (int i = 0; i < sampleCount; i++)
            {
                int randomIndex = Random.Shared.Next(keys.Count);
                int candidateKey = keys[randomIndex];

                if (_cache.TryGetValue(candidateKey, out LruCacheEntry candidate))
                {
                    long ticks = Interlocked.Read(ref candidate.LastAccessedTicks);
                    if (ticks < oldestTicks)
                    {
                        oldestTicks = ticks;
                        oldestKey = candidateKey;
                    }
                }
            }

            if (oldestKey >= 0)
            {
                _cache.TryRemove(oldestKey, out _);
            }
        }
    }
}
