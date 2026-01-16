using System;
using System.Buffers.Binary;
using System.Numerics;
using GaldrDbEngine.IO;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

internal class FreeSpaceMap
{
    private IPageIO _pageIO;
    private readonly int _startPage;
    private readonly int _pageCount;
    private readonly int _pageSize;
    private int _totalPages;
    private byte[] _fsm;

    public FreeSpaceMap(IPageIO pageIO, int startPage, int pageCount, int totalPages, int pageSize)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _totalPages = totalPages;
        _pageSize = pageSize;

        int fsmSizeBytes = (totalPages * 2 + 7) / 8;
        _fsm = new byte[fsmSizeBytes];
    }
    
    public void SetPageIO(IPageIO pageIO)
    {
        _pageIO = pageIO;
    }

    public FreeSpaceLevel GetFreeSpaceLevel(int pageId)
    {
        if (pageId < 0 || pageId >= _totalPages)
        {
            throw new ArgumentOutOfRangeException(nameof(pageId));
        }

        int bitPosition = pageId * 2;
        int byteIndex = bitPosition / 8;
        int bitOffset = bitPosition % 8;

        byte value = (byte)((_fsm[byteIndex] >> bitOffset) & 0x03);
        FreeSpaceLevel result = (FreeSpaceLevel)value;

        return result;
    }

    public void SetFreeSpaceLevel(int pageId, FreeSpaceLevel level)
    {
        if (pageId < 0 || pageId >= _totalPages)
        {
            throw new ArgumentOutOfRangeException(nameof(pageId));
        }

        int bitPosition = pageId * 2;
        int byteIndex = bitPosition / 8;
        int bitOffset = bitPosition % 8;

        byte mask = (byte)(0x03 << bitOffset);
        byte clearedByte = (byte)(_fsm[byteIndex] & ~mask);
        byte newValue = (byte)(clearedByte | ((byte)level << bitOffset));

        _fsm[byteIndex] = newValue;
    }

    public int FindPageWithSpace(FreeSpaceLevel minLevel)
    {
        int result = -1;
        int byteLength = _fsm.Length;
        int ulongCount = byteLength / 8;

        // Each ulong contains 32 pages (64 bits / 2 bits per page)
        // Mask to isolate the low bit of each 2-bit pair
        const ulong lowBitMask = 0x5555555555555555UL;

        for (int i = 0; i < ulongCount; i++)
        {
            int offset = i * 8;
            ulong chunk = BinaryPrimitives.ReadUInt64LittleEndian(_fsm.AsSpan(offset, 8));

            ulong mask;
            if (minLevel == FreeSpaceLevel.High)
            {
                // Both bits must be 1 (11): AND chunk with shifted version, isolate low bits
                mask = (chunk & (chunk >> 1)) & lowBitMask;
            }
            else if (minLevel == FreeSpaceLevel.Medium)
            {
                // High bit must be 1 (10 or 11): shift to get high bits, isolate
                mask = (chunk >> 1) & lowBitMask;
            }
            else
            {
                // At least one bit must be 1 (01, 10, or 11): OR chunk with shifted version
                mask = (chunk | (chunk >> 1)) & lowBitMask;
            }

            if (mask != 0)
            {
                int bitPosition = BitOperations.TrailingZeroCount(mask);
                int pageWithinChunk = bitPosition / 2;
                int pageId = i * 32 + pageWithinChunk;

                if (pageId < _totalPages)
                {
                    result = pageId;
                }
                break;
            }
        }

        // Handle remaining bytes that don't fill a complete ulong
        if (result == -1)
        {
            int startPage = ulongCount * 32;
            for (int pageId = startPage; pageId < _totalPages; pageId++)
            {
                FreeSpaceLevel level = GetFreeSpaceLevel(pageId);

                if (level >= minLevel)
                {
                    result = pageId;
                    break;
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

        int newFsmSizeBytes = (newTotalPages * 2 + 7) / 8;
        byte[] newFsm = new byte[newFsmSizeBytes];
        Array.Copy(_fsm, 0, newFsm, 0, _fsm.Length);

        _fsm = newFsm;
        _totalPages = newTotalPages;
    }

    public void LoadFromDisk()
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int fsmSizeBytes = (_totalPages * 2 + 7) / 8;
            int offset = 0;

            for (int i = 0; i < _pageCount; i++)
            {
                _pageIO.ReadPage(_startPage + i, buffer);
                int bytesToCopy = Math.Min(_pageSize, fsmSizeBytes - offset);

                Array.Copy(buffer, 0, _fsm, offset, bytesToCopy);
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
            int fsmSizeBytes = (_totalPages * 2 + 7) / 8;
            int offset = 0;

            for (int i = 0; i < _pageCount; i++)
            {
                Array.Clear(buffer, 0, _pageSize);
                int bytesToCopy = Math.Min(_pageSize, fsmSizeBytes - offset);

                Array.Copy(_fsm, offset, buffer, 0, bytesToCopy);
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
