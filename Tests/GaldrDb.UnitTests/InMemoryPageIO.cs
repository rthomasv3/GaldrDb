using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;

namespace GaldrDb.UnitTests;

internal class InMemoryPageIO : IPageIO
{
    private readonly int _pageSize;
    private readonly Dictionary<int, byte[]> _pages;
    private bool _disposed;

    public InMemoryPageIO(int pageSize)
    {
        _pageSize = pageSize;
        _pages = new Dictionary<int, byte[]>();
        _disposed = false;
    }

    public int PageCount => _pages.Count;

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(InMemoryPageIO));
        }

        if (_pages.TryGetValue(pageId, out byte[] data))
        {
            data.AsSpan().CopyTo(destination);
        }
        else
        {
            destination.Clear();
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(InMemoryPageIO));
        }

        // Sparse storage: don't store all-zero pages
        bool allZeros = true;
        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] != 0)
            {
                allZeros = false;
                break;
            }
        }

        if (allZeros)
        {
            _pages.Remove(pageId);
        }
        else
        {
            byte[] pageData = new byte[_pageSize];
            data.CopyTo(pageData);
            _pages[pageId] = pageData;
        }
    }

    public void Flush()
    {
        // No-op for in-memory storage
    }

    public void Close()
    {
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
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _disposed = true;
        _pages.Clear();
    }
}
