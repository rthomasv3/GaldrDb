using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Utilities;
using Microsoft.Win32.SafeHandles;

namespace GaldrDbEngine.WAL;

internal sealed class FileWalStreamIO : IWalStreamIO
{
    private readonly SafeFileHandle _fileHandle;
    private readonly AsyncReaderWriterLock _rwLock;
    private bool _disposed;

    public FileWalStreamIO(string filePath, bool createNew)
    {
        FileMode fileMode = createNew ? FileMode.CreateNew : FileMode.Open;
        _fileHandle = File.OpenHandle(filePath, fileMode, FileAccess.ReadWrite, FileShare.Read);
        _rwLock = new AsyncReaderWriterLock();
        _disposed = false;
    }

    public long Length
    {
        get { return RandomAccess.GetLength(_fileHandle); }
    }

    public int ReadAtPosition(long position, Span<byte> buffer)
    {
        _rwLock.EnterReadLock();
        try
        {
            int totalBytesRead = 0;
            int remaining = buffer.Length;

            while (remaining > 0)
            {
                int bytesRead = RandomAccess.Read(_fileHandle, buffer.Slice(totalBytesRead, remaining), position + totalBytesRead);

                if (bytesRead == 0)
                {
                    break;
                }

                totalBytesRead += bytesRead;
                remaining -= bytesRead;
            }

            return totalBytesRead;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public async Task<int> ReadAtPositionAsync(long position, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _rwLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int totalBytesRead = 0;
            int remaining = buffer.Length;

            while (remaining > 0)
            {
                int bytesRead = await RandomAccess.ReadAsync(_fileHandle, buffer.Slice(totalBytesRead, remaining), position + totalBytesRead, cancellationToken).ConfigureAwait(false);

                if (bytesRead == 0)
                {
                    break;
                }

                totalBytesRead += bytesRead;
                remaining -= bytesRead;
            }

            return totalBytesRead;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public void WriteAtPosition(long position, ReadOnlySpan<byte> buffer)
    {
        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.Write(_fileHandle, buffer, position);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public async Task WriteAtPositionAsync(long position, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await _rwLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await RandomAccess.WriteAsync(_fileHandle, buffer, position, cancellationToken).ConfigureAwait(false);
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

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    public void SetLength(long length)
    {
        _rwLock.EnterWriteLock();
        try
        {
            RandomAccess.SetLength(_fileHandle, length);
        }
        finally
        {
            _rwLock.ExitWriteLock();
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
}
