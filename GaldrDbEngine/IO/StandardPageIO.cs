using System;
using System.IO;

namespace GaldrDbCore.IO;

public class StandardPageIO : IPageIO
{
    private readonly string _filePath;
    private readonly int _pageSize;
    private FileStream _fileStream;

    public StandardPageIO(string filePath, int pageSize, bool createNew)
    {
        _filePath = filePath;
        _pageSize = pageSize;

        FileMode fileMode = FileMode.Open;

        if (createNew)
        {
            fileMode = FileMode.CreateNew;
        }

        _fileStream = new FileStream(_filePath, fileMode, FileAccess.ReadWrite, FileShare.Read);
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        if (destination.Length < _pageSize)
        {
            throw new ArgumentException($"Destination length {destination.Length} is smaller than page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _fileStream.Seek(offset, SeekOrigin.Begin);

        int totalBytesRead = 0;

        while (totalBytesRead < _pageSize)
        {
            int bytesRead = _fileStream.Read(destination.Slice(totalBytesRead, _pageSize - totalBytesRead));

            if (bytesRead == 0)
            {
                break;
            }

            totalBytesRead += bytesRead;
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        if (data.Length != _pageSize)
        {
            throw new ArgumentException($"Data length {data.Length} does not match page size {_pageSize}");
        }

        long offset = (long)pageId * _pageSize;

        _fileStream.Seek(offset, SeekOrigin.Begin);
        _fileStream.Write(data);
    }

    public void Flush()
    {
        _fileStream.Flush();
    }

    public void Close()
    {
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
