using System;
using GaldrDbCore.IO;

namespace GaldrDbCore.Pages;

public class Bitmap
{
    private readonly IPageIO _pageIO;
    private readonly int _startPage;
    private readonly int _pageCount;
    private readonly int _totalPages;
    private byte[] _bitmap;

    public Bitmap(IPageIO pageIO, int startPage, int pageCount, int totalPages)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _totalPages = totalPages;

        int bitmapSizeBytes = (totalPages + 7) / 8;
        _bitmap = new byte[bitmapSizeBytes];
    }

    public bool IsAllocated(int pageId)
    {
        if (pageId < 0 || pageId >= _totalPages)
        {
            throw new ArgumentOutOfRangeException(nameof(pageId));
        }

        int byteIndex = pageId / 8;
        int bitIndex = pageId % 8;
        byte mask = (byte)(1 << bitIndex);
        bool result = (_bitmap[byteIndex] & mask) != 0;

        return result;
    }

    public void AllocatePage(int pageId)
    {
        if (pageId < 0 || pageId >= _totalPages)
        {
            throw new ArgumentOutOfRangeException(nameof(pageId));
        }

        int byteIndex = pageId / 8;
        int bitIndex = pageId % 8;
        byte mask = (byte)(1 << bitIndex);

        _bitmap[byteIndex] = (byte)(_bitmap[byteIndex] | mask);
    }

    public void DeallocatePage(int pageId)
    {
        if (pageId < 0 || pageId >= _totalPages)
        {
            throw new ArgumentOutOfRangeException(nameof(pageId));
        }

        int byteIndex = pageId / 8;
        int bitIndex = pageId % 8;
        byte mask = (byte)(1 << bitIndex);

        _bitmap[byteIndex] = (byte)(_bitmap[byteIndex] & ~mask);
    }

    public int FindFreePage()
    {
        int result = -1;

        for (int pageId = 0; pageId < _totalPages; pageId++)
        {
            if (!IsAllocated(pageId))
            {
                result = pageId;
                break;
            }
        }

        return result;
    }

    public void LoadFromDisk()
    {
        int pageSize = _pageIO.ReadPage(0).Length;
        int bytesPerPage = pageSize;
        int bitmapSizeBytes = (_totalPages + 7) / 8;
        int offset = 0;

        for (int i = 0; i < _pageCount; i++)
        {
            byte[] pageData = _pageIO.ReadPage(_startPage + i);
            int bytesToCopy = Math.Min(bytesPerPage, bitmapSizeBytes - offset);

            Array.Copy(pageData, 0, _bitmap, offset, bytesToCopy);
            offset += bytesToCopy;
        }
    }

    public void WriteToDisk()
    {
        int pageSize = _pageIO.ReadPage(0).Length;
        int bytesPerPage = pageSize;
        int bitmapSizeBytes = (_totalPages + 7) / 8;
        int offset = 0;

        for (int i = 0; i < _pageCount; i++)
        {
            byte[] pageData = new byte[pageSize];
            int bytesToCopy = Math.Min(bytesPerPage, bitmapSizeBytes - offset);

            Array.Copy(_bitmap, offset, pageData, 0, bytesToCopy);
            _pageIO.WritePage(_startPage + i, pageData);

            offset += bytesToCopy;
        }
    }
}
