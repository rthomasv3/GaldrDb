using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class CollectionsMetadata
{
    private readonly IPageIO _pageIO;
    private readonly int _pageSize;
    private readonly int _usablePageSize;
    private int _startPage;
    private int _pageCount;
    private readonly Dictionary<string, CollectionEntry> _collections;

    public CollectionsMetadata(IPageIO pageIO, int startPage, int pageCount, int pageSize, int usablePageSize = 0)
    {
        _pageIO = pageIO;
        _startPage = startPage;
        _pageCount = pageCount;
        _pageSize = pageSize;
        _usablePageSize = usablePageSize > 0 ? usablePageSize : pageSize;
        _collections = new Dictionary<string, CollectionEntry>();
    }

    public void LoadFromDisk()
    {
        int totalBufferSize = _pageCount * _usablePageSize;
        byte[] combinedBuffer = BufferPool.Rent(totalBufferSize);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);

        try
        {
            int bufferOffset = 0;
            for (int i = 0; i < _pageCount; i++)
            {
                _pageIO.ReadPage(_startPage + i, pageBuffer);
                int bytesToCopy = Math.Min(_usablePageSize, totalBufferSize - bufferOffset);
                Array.Copy(pageBuffer, 0, combinedBuffer, bufferOffset, bytesToCopy);
                bufferOffset += bytesToCopy;
            }

            _collections.Clear();

            int offset = 0;
            int collectionCount = BinaryHelper.ReadInt32LE(combinedBuffer, offset);
            offset += 4;

            for (int i = 0; i < collectionCount; i++)
            {
                int bytesRead = 0;
                CollectionEntry entry = CollectionEntry.Deserialize(combinedBuffer, offset, out bytesRead);
                _collections[entry.Name] = entry;
                offset += bytesRead;
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
            BufferPool.Return(combinedBuffer);
        }
    }

    public void WriteToDisk()
    {
        int totalSize = CalculateSerializedSize();
        int pagesNeeded = (totalSize + _usablePageSize - 1) / _usablePageSize;

        if (pagesNeeded > _pageCount)
        {
            throw new InvalidOperationException(
                $"Collections metadata requires {pagesNeeded} pages but only {_pageCount} allocated. " +
                "Call GrowCollectionsMetadata() first.");
        }

        int totalBufferSize = _pageCount * _usablePageSize;
        byte[] combinedBuffer = BufferPool.Rent(totalBufferSize);
        byte[] pageBuffer = BufferPool.Rent(_pageSize);

        try
        {
            Array.Clear(combinedBuffer, 0, totalBufferSize);

            int offset = 0;
            BinaryHelper.WriteInt32LE(combinedBuffer, offset, _collections.Count);
            offset += 4;

            foreach (KeyValuePair<string, CollectionEntry> kvp in _collections)
            {
                offset += kvp.Value.SerializeTo(combinedBuffer, offset);
            }

            int bufferOffset = 0;
            for (int i = 0; i < _pageCount; i++)
            {
                Array.Clear(pageBuffer, 0, _pageSize);
                int bytesToCopy = Math.Min(_usablePageSize, totalBufferSize - bufferOffset);
                Array.Copy(combinedBuffer, bufferOffset, pageBuffer, 0, bytesToCopy);
                _pageIO.WritePage(_startPage + i, pageBuffer);
                bufferOffset += bytesToCopy;
            }
        }
        finally
        {
            BufferPool.Return(pageBuffer);
            BufferPool.Return(combinedBuffer);
        }
    }

    public CollectionEntry AddCollection(string name, int rootPage)
    {
        CollectionEntry entry = new CollectionEntry
        {
            Name = name,
            RootPage = rootPage,
            DocumentCount = 0,
            NextId = 1
        };

        _collections[name] = entry;

        return entry;
    }

    public CollectionEntry FindCollection(string name)
    {
        CollectionEntry result = null;

        if (_collections.TryGetValue(name, out CollectionEntry entry))
        {
            result = entry;
        }

        return result;
    }

    public void UpdateCollection(CollectionEntry entry)
    {
        if (_collections.ContainsKey(entry.Name))
        {
            _collections[entry.Name] = entry;
        }
    }

    public void RemoveCollection(string name)
    {
        _collections.Remove(name);
    }

    public int GetCollectionCount()
    {
        return _collections.Count;
    }

    public List<CollectionEntry> GetAllCollections()
    {
        List<CollectionEntry> result = new List<CollectionEntry>(_collections.Count);

        foreach (KeyValuePair<string, CollectionEntry> kvp in _collections)
        {
            result.Add(kvp.Value);
        }

        return result;
    }

    public int CalculateSerializedSize()
    {
        int size = 4;

        foreach (KeyValuePair<string, CollectionEntry> kvp in _collections)
        {
            size += kvp.Value.GetSerializedSize();
        }

        return size;
    }

    public int GetPagesNeeded()
    {
        int totalSize = CalculateSerializedSize();
        int pagesNeeded = (totalSize + _usablePageSize - 1) / _usablePageSize;

        return pagesNeeded;
    }

    public int GetCurrentPageCount()
    {
        return _pageCount;
    }

    public int GetStartPage()
    {
        return _startPage;
    }

    public void SetPageAllocation(int startPage, int pageCount)
    {
        _startPage = startPage;
        _pageCount = pageCount;
    }
}
