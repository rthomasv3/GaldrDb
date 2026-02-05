using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.IO;

internal class MmapPageIO : IPageIO
{
    private readonly string _filePath;
    private readonly int _pageSize;
    private readonly AsyncReaderWriterLock _rwLock;
    private FileStream _fileStream;
    private MemoryMappedFile _memoryMappedFile;
    private MemoryMappedViewAccessor _accessor;
    private bool _disposed;

    public MmapPageIO(string filePath, int pageSize, long initialSize, bool createNew)
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

        _fileStream = new FileStream(_filePath, fileMode, FileAccess.ReadWrite, FileShare.Read);

        if (createNew && initialSize > 0)
        {
            _fileStream.SetLength(initialSize);
        }

        long fileSize = Math.Max(_fileStream.Length, _pageSize);

        _memoryMappedFile = MemoryMappedFile.CreateFromFile(
            _fileStream,
            null,
            fileSize,
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None,
            true);

        _accessor = _memoryMappedFile.CreateViewAccessor();
    }

    public static bool IsMmapSupported()
    {
        bool result = true;

        try
        {
            string tempPath = Path.Combine(Path.GetTempPath(), $"mmap_test_{Guid.NewGuid()}.tmp");

            using (FileStream fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read))
            {
                fs.SetLength(4096);

                using (MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(fs, null, 4096, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, true))
                {
                    using (MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor())
                    {
                        accessor.Write(0, (byte)42);
                        byte value = accessor.ReadByte(0);

                        if (value != 42)
                        {
                            result = false;
                        }
                    }
                }
            }

            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
        catch
        {
            result = false;
        }

        return result;
    }

    public void ReadPage(int pageId, Span<byte> destination, TransactionContext context = null)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterReadLock();
        try
        {
            long currentFileSize = _fileStream.Length;

            if (offset + _pageSize > currentFileSize)
            {
                destination.Slice(0, _pageSize).Clear();
            }
            else
            {
                byte[] tempBuffer = BufferPool.Rent(_pageSize);
                try
                {
                    _accessor.ReadArray(offset, tempBuffer, 0, _pageSize);
                    tempBuffer.AsSpan(0, _pageSize).CopyTo(destination);
                }
                finally
                {
                    BufferPool.Return(tempBuffer);
                }
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data, TransactionContext context = null)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _rwLock.EnterWriteLock();
        try
        {
            long requiredSize = offset + _pageSize;
            long currentFileSize = _fileStream.Length;

            // SetLength should be called before writes to new pages, but handle edge case
            if (requiredSize > currentFileSize)
            {
                _accessor?.Dispose();
                _memoryMappedFile?.Dispose();

                _fileStream.SetLength(requiredSize);

                _memoryMappedFile = MemoryMappedFile.CreateFromFile(
                    _fileStream,
                    null,
                    requiredSize,
                    MemoryMappedFileAccess.ReadWrite,
                    HandleInheritability.None,
                    true);

                _accessor = _memoryMappedFile.CreateViewAccessor();
            }

            byte[] tempBuffer = BufferPool.Rent(_pageSize);
            try
            {
                data.CopyTo(tempBuffer);
                _accessor.WriteArray(offset, tempBuffer, 0, _pageSize);
            }
            finally
            {
                BufferPool.Return(tempBuffer);
            }
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
            _accessor?.Flush();
            _fileStream?.Flush();
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
            long currentSize = _fileStream.Length;
            if (newSize > currentSize)
            {
                _accessor?.Dispose();
                _memoryMappedFile?.Dispose();

                _fileStream.SetLength(newSize);

                _memoryMappedFile = MemoryMappedFile.CreateFromFile(
                    _fileStream,
                    null,
                    newSize,
                    MemoryMappedFileAccess.ReadWrite,
                    HandleInheritability.None,
                    true);

                _accessor = _memoryMappedFile.CreateViewAccessor();
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public void Close()
    {
        if (_accessor != null)
        {
            _accessor.Dispose();
            _accessor = null;
        }

        if (_memoryMappedFile != null)
        {
            _memoryMappedFile.Dispose();
            _memoryMappedFile = null;
        }

        if (_fileStream != null)
        {
            _fileStream.Close();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            Close();
            _rwLock.Dispose();

            if (_fileStream != null)
            {
                _fileStream.Dispose();
            }
        }
    }

    public Task ReadPageAsync(int pageId, Memory<byte> destination, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        ReadPage(pageId, destination.Span, context);
        return Task.CompletedTask;
    }

    public Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, TransactionContext context = null, CancellationToken cancellationToken = default)
    {
        WritePage(pageId, data.Span, context);
        return Task.CompletedTask;
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }
}
