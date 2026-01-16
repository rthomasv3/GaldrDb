using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

namespace GaldrDbEngine.IO;

public class StandardPageIO : IPageIO
{
    private readonly int _pageSize;
    private readonly SafeFileHandle _fileHandle;
    private readonly ReaderWriterLockSlim _rwLock;
    private bool _disposed;

    public StandardPageIO(string filePath, int pageSize, bool createNew)
    {
        _pageSize = pageSize;
        _rwLock = new ReaderWriterLockSlim();
        _disposed = false;

        FileMode fileMode = FileMode.Open;

        if (createNew)
        {
            fileMode = FileMode.CreateNew;
        }

        _fileHandle = File.OpenHandle(filePath, fileMode, FileAccess.ReadWrite, FileShare.Read);
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterReadLock();
        try
        {
            int totalBytesRead = 0;

            while (totalBytesRead < _pageSize)
            {
                int bytesRead = RandomAccess.Read(_fileHandle, destination.Slice(totalBytesRead, _pageSize - totalBytesRead), offset + totalBytesRead);

                if (bytesRead == 0)
                {
                    break;
                }

                totalBytesRead += bytesRead;
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.Write(_fileHandle, data, offset);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public void Flush()
    {
        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.FlushToDisk(_fileHandle);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public void Close()
    {
        if (!_disposed)
        {
            _fileHandle.Close();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _rwLock.Dispose();
            _fileHandle.Dispose();
        }
    }

    public async Task ReadPageAsync(int pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterReadLock();
        try
        {
            int totalBytesRead = 0;

            while (totalBytesRead < _pageSize)
            {
                int bytesRead = await RandomAccess.ReadAsync(_fileHandle, destination.Slice(totalBytesRead, _pageSize - totalBytesRead), offset + totalBytesRead, cancellationToken);

                if (bytesRead == 0)
                {
                    break;
                }

                totalBytesRead += bytesRead;
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public async Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterWriteLock();
        try
        {
            await RandomAccess.WriteAsync(_fileHandle, data, offset, cancellationToken);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        // RandomAccess doesn't have an async flush, use sync version
        Flush();
        return Task.CompletedTask;
    }
}
