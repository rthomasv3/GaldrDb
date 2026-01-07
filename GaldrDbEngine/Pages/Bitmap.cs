using System;
using GaldrDbCore.IO;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Pages;

public class Bitmap
{
    private readonly IPageIO _pageIO;
    private readonly int _startPage;
    private readonly int _pageCount;
    private readonly int _pageSize;
    private int _totalPages;
    private byte[] _bitmap;

    public Bitmap(IPageIO pageIO, int startPage, int pageCount, int totalPages, int pageSize)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _totalPages = totalPages;
        _pageSize = pageSize;

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

    public void Resize(int newTotalPages)
    {
        if (newTotalPages <= _totalPages)
        {
            return;
        }

        int newBitmapSizeBytes = (newTotalPages + 7) / 8;
        byte[] newBitmap = new byte[newBitmapSizeBytes];
        Array.Copy(_bitmap, 0, newBitmap, 0, _bitmap.Length);

        _bitmap = newBitmap;
        _totalPages = newTotalPages;
    }

    public void LoadFromDisk()
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int bitmapSizeBytes = (_totalPages + 7) / 8;
            int offset = 0;

            for (int i = 0; i < _pageCount; i++)
            {
                _pageIO.ReadPage(_startPage + i, buffer);
                int bytesToCopy = Math.Min(_pageSize, bitmapSizeBytes - offset);

                Array.Copy(buffer, 0, _bitmap, offset, bytesToCopy);
                offset += bytesToCopy;
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    public void WriteToDisk()
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int bitmapSizeBytes = (_totalPages + 7) / 8;
            int offset = 0;

            for (int i = 0; i < _pageCount; i++)
            {
                Array.Clear(buffer, 0, _pageSize);
                int bytesToCopy = Math.Min(_pageSize, bitmapSizeBytes - offset);

                Array.Copy(_bitmap, offset, buffer, 0, bytesToCopy);
                _pageIO.WritePage(_startPage + i, buffer);

                offset += bytesToCopy;
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }
}
