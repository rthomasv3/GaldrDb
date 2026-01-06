using System;
using GaldrDbCore.IO;

namespace GaldrDbCore.Pages;

public class FreeSpaceMap
{
    private readonly IPageIO _pageIO;
    private readonly int _startPage;
    private readonly int _pageCount;
    private readonly int _totalPages;
    private byte[] _fsm;

    public FreeSpaceMap(IPageIO pageIO, int startPage, int pageCount, int totalPages)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _totalPages = totalPages;

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

    public void LoadFromDisk()
    {
        int pageSize = _pageIO.ReadPage(0).Length;
        int bytesPerPage = pageSize;
        int fsmSizeBytes = (_totalPages * 2 + 7) / 8;
        int offset = 0;

        for (int i = 0; i < _pageCount; i++)
        {
            byte[] pageData = _pageIO.ReadPage(_startPage + i);
            int bytesToCopy = Math.Min(bytesPerPage, fsmSizeBytes - offset);

            Array.Copy(pageData, 0, _fsm, offset, bytesToCopy);
            offset += bytesToCopy;
        }
    }

    public void WriteToDisk()
    {
        int pageSize = _pageIO.ReadPage(0).Length;
        int bytesPerPage = pageSize;
        int fsmSizeBytes = (_totalPages * 2 + 7) / 8;
        int offset = 0;

        for (int i = 0; i < _pageCount; i++)
        {
            byte[] pageData = new byte[pageSize];
            int bytesToCopy = Math.Min(bytesPerPage, fsmSizeBytes - offset);

            Array.Copy(_fsm, offset, pageData, 0, bytesToCopy);
            _pageIO.WritePage(_startPage + i, pageData);

            offset += bytesToCopy;
        }
    }
}
