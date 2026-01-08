using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WalPageIO : IPageIO
{
    private readonly IPageIO _innerPageIO;
    private readonly WriteAheadLog _wal;
    private readonly int _pageSize;
    private readonly Dictionary<int, byte[]> _walPageCache;
    private ulong _currentTxId;
    private bool _disposed;

    public WalPageIO(IPageIO innerPageIO, WriteAheadLog wal, int pageSize)
    {
        _innerPageIO = innerPageIO;
        _wal = wal;
        _pageSize = pageSize;
        _walPageCache = new Dictionary<int, byte[]>();
        _currentTxId = 1;
        _disposed = false;
    }

    public ulong CurrentTxId
    {
        get { return _currentTxId; }
        set { _currentTxId = value; }
    }

    public void ReadPage(int pageId, Span<byte> destination)
    {
        // Check WAL cache first for uncommitted/recent writes
        if (_walPageCache.TryGetValue(pageId, out byte[] cachedPage))
        {
            cachedPage.AsSpan().CopyTo(destination);
        }
        else
        {
            _innerPageIO.ReadPage(pageId, destination);
        }
    }

    public void WritePage(int pageId, ReadOnlySpan<byte> data)
    {
        // Write to WAL with auto-commit (each write is its own transaction for now)
        byte[] pageData = data.ToArray();
        byte pageType = DeterminePageType(pageData);

        _wal.WriteFrame(_currentTxId, pageId, pageType, pageData, WalFrameFlags.Commit);

        // Cache the page for subsequent reads
        _walPageCache[pageId] = pageData;

        // Increment transaction ID for next auto-commit operation
        _currentTxId++;
    }

    public void WritePageWithCommit(int pageId, ReadOnlySpan<byte> data)
    {
        // Write to WAL with commit flag
        byte[] pageData = data.ToArray();
        byte pageType = DeterminePageType(pageData);

        _wal.WriteFrame(_currentTxId, pageId, pageType, pageData, WalFrameFlags.Commit);

        // Cache the page for subsequent reads
        _walPageCache[pageId] = pageData;

        // Increment transaction ID for next transaction
        _currentTxId++;
    }

    public void CommitCurrentTransaction()
    {
        // Write a commit frame with no page data to mark transaction end
        _wal.WriteFrame(_currentTxId, -1, 0, Array.Empty<byte>(), WalFrameFlags.Commit);
        _wal.Flush();
        _currentTxId++;
    }

    public void Flush()
    {
        _wal.Flush();
        _innerPageIO.Flush();
    }

    public void Close()
    {
        _innerPageIO.Close();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _innerPageIO.Dispose();
        }
    }

    public void Checkpoint()
    {
        // Apply all cached pages to the underlying storage
        foreach (KeyValuePair<int, byte[]> entry in _walPageCache)
        {
            _innerPageIO.WritePage(entry.Key, entry.Value);
        }

        _innerPageIO.Flush();

        // Clear the cache after checkpoint
        _walPageCache.Clear();

        // Truncate the WAL
        _wal.Truncate();
    }

    public void ApplyWalFrames(List<WalFrame> frames)
    {
        foreach (WalFrame frame in frames)
        {
            if (frame.PageId >= 0 && frame.Data.Length > 0)
            {
                _walPageCache[frame.PageId] = frame.Data;
            }
        }
    }

    public void ClearCache()
    {
        _walPageCache.Clear();
    }

    private static byte DeterminePageType(byte[] pageData)
    {
        byte pageType = PageConstants.PAGE_TYPE_DOCUMENT;

        if (pageData.Length >= 2)
        {
            ushort typeMarker = BinaryHelper.ReadUInt16LE(pageData, 0);

            if (typeMarker == PageConstants.PAGE_TYPE_BTREE)
            {
                pageType = PageConstants.PAGE_TYPE_BTREE;
            }
            else if (typeMarker == PageConstants.PAGE_TYPE_DOCUMENT)
            {
                pageType = PageConstants.PAGE_TYPE_DOCUMENT;
            }
        }

        return pageType;
    }
}
