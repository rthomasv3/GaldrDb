using System;
using GaldrDbCore.IO;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Pages;

public class FreeSpaceMap
{
    private readonly IPageIO _pageIO;
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

        for (int pageId = 0; pageId < _totalPages; pageId++)
        {
            FreeSpaceLevel level = GetFreeSpaceLevel(pageId);

            if (level >= minLevel)
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
