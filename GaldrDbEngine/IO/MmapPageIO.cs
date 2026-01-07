using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace GaldrDbCore.IO;

public class MmapPageIO : IPageIO
{
    private readonly string _filePath;
    private readonly int _pageSize;
    private FileStream _fileStream;
    private MemoryMappedFile _memoryMappedFile;
    private MemoryMappedViewAccessor _accessor;

    public MmapPageIO(string filePath, int pageSize, long initialSize, bool createNew)
    {
        _filePath = filePath;
        _pageSize = pageSize;

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

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;
        long currentFileSize = _fileStream.Length;

        if (offset + _pageSize > currentFileSize)
        {
            destination.Slice(0, _pageSize).Clear();
        }
        else
        {
            byte[] tempBuffer = new byte[_pageSize];
            _accessor.ReadArray(offset, tempBuffer, 0, _pageSize);
            tempBuffer.AsSpan().CopyTo(destination);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;
        long requiredSize = offset + _pageSize;
        long currentFileSize = _fileStream.Length;

        if (requiredSize > currentFileSize)
        {
            long newSize = Math.Max(requiredSize, currentFileSize * 2);

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

        byte[] tempBuffer = data.ToArray();
        _accessor.WriteArray(offset, tempBuffer, 0, tempBuffer.Length);
    }

    public void Flush()
    {
        _accessor?.Flush();
        _fileStream?.Flush();
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
        Close();

        if (_fileStream != null)
        {
            _fileStream.Dispose();
        }
    }
}
