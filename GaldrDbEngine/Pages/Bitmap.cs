using System;
using System.Buffers.Binary;
using System.Numerics;
using GaldrDbEngine.IO;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

internal class Bitmap
{
    private IPageIO _pageIO;
    private int _startPage;
    private int _pageCount;
    private readonly int _pageSize;
    private readonly int _usablePageSize;
    private int _totalPages;
    private byte[] _bitmap;

    public Bitmap(IPageIO pageIO, int startPage, int pageCount, int totalPages, int pageSize, int usablePageSize = 0)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _totalPages = totalPages;
        _pageSize = pageSize;
        _usablePageSize = usablePageSize > 0 ? usablePageSize : pageSize;

        int bitmapSizeBytes = (totalPages + 7) / 8;
        _bitmap = new byte[bitmapSizeBytes];
    }

    public int StartPage
    {
        get { return _startPage; }
    }

    public int PageCount
    {
        get { return _pageCount; }
    }

    public void SetPageIO(IPageIO pageIO)
    {
        _pageIO = pageIO;
    }

    public void SetPageAllocation(int startPage, int pageCount)
    {
        _startPage = startPage;
        _pageCount = pageCount;
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

        return (_bitmap[byteIndex] & mask) != 0;
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

    public int FindFreePage(int hint)
    {
        int result = -1;
        int byteLength = _bitmap.Length;
        int ulongCount = byteLength / 8;

        // Clamp hint to valid range
        if (hint < 0)
        {
            hint = 0;
        }
        else if (hint >= _totalPages)
        {
            hint = 0;
        }

        // Start scanning from the hint's ulong-aligned position
        int startUlong = hint / 64;

        // Process 64 bits at a time using hardware intrinsics
        for (int i = startUlong; i < ulongCount && result == -1; i++)
        {
            int offset = i * 8;
            ulong chunk = BinaryPrimitives.ReadUInt64LittleEndian(_bitmap.AsSpan(offset, 8));

            if (chunk != ulong.MaxValue)
            {
                // Invert to find first 0 bit (becomes first 1 bit after inversion)
                ulong inverted = ~chunk;
                int bitPosition = BitOperations.TrailingZeroCount(inverted);
                int pageId = i * 64 + bitPosition;

                if (pageId < _totalPages)
                {
                    result = pageId;
                }
            }
        }

        // Handle remaining bytes that don't fill a complete ulong
        if (result == -1)
        {
            int startByte = Math.Max(ulongCount * 8, (hint / 8));
            for (int byteIndex = startByte; byteIndex < byteLength && result == -1; byteIndex++)
            {
                byte b = _bitmap[byteIndex];
                if (b != 0xFF)
                {
                    int bitPosition = BitOperations.TrailingZeroCount((uint)(byte)~b);
                    int pageId = byteIndex * 8 + bitPosition;

                    if (pageId < _totalPages)
                    {
                        result = pageId;
                    }
                }
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
                int bytesToCopy = Math.Min(_usablePageSize, bitmapSizeBytes - offset);

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
                int bytesToCopy = Math.Min(_usablePageSize, bitmapSizeBytes - offset);

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
