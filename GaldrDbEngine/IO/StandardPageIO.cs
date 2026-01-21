using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Utilities;
using Microsoft.Win32.SafeHandles;

namespace GaldrDbEngine.IO;

internal class StandardPageIO : IPageIO
{
    private readonly string _filePath;
    private readonly int _pageSize;
    private readonly SafeFileHandle _fileHandle;
    private readonly AsyncReaderWriterLock _rwLock;
    private bool _disposed;

    public StandardPageIO(string filePath, int pageSize, bool createNew)
    {
        _filePath = filePath;
        _pageSize = pageSize;
        _rwLock = new AsyncReaderWriterLock();
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

    public void SetLength(long newSize)
    {
        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.SetLength(_fileHandle, newSize);
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

        await _rwLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int totalBytesRead = 0;

            while (totalBytesRead < _pageSize)
            {
                int bytesRead = await RandomAccess.ReadAsync(_fileHandle, destination.Slice(totalBytesRead, _pageSize - totalBytesRead), offset + totalBytesRead, cancellationToken).ConfigureAwait(false);

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

        await _rwLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await RandomAccess.WriteAsync(_fileHandle, data, offset, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }
}
