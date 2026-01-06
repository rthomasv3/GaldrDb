using System;
using System.Collections.Generic;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Storage;

public class CollectionsMetadata
{
    private readonly IPageIO _pageIO;
    private readonly int _metadataPageId;
    private readonly int _pageSize;
    private readonly List<CollectionEntry> _collections;

    public CollectionsMetadata(IPageIO pageIO, int metadataPageId, int pageSize)
    {
        _pageIO = pageIO;
        _metadataPageId = metadataPageId;
        _pageSize = pageSize;
        _collections = new List<CollectionEntry>();
    }

    public void LoadFromDisk()
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(_metadataPageId, buffer);

            _collections.Clear();

            int offset = 0;
            int collectionCount = BinaryHelper.ReadInt32LE(buffer, offset);
            offset += 4;

            for (int i = 0; i < collectionCount; i++)
            {
                int bytesRead = 0;
                CollectionEntry entry = CollectionEntry.Deserialize(buffer, offset, out bytesRead);
                _collections.Add(entry);
                offset += bytesRead;
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
            Array.Clear(buffer, 0, _pageSize);

            int offset = 0;
            BinaryHelper.WriteInt32LE(buffer, offset, _collections.Count);
            offset += 4;

            for (int i = 0; i < _collections.Count; i++)
            {
                byte[] entryBytes = _collections[i].Serialize();
                Array.Copy(entryBytes, 0, buffer, offset, entryBytes.Length);
                offset += entryBytes.Length;
            }

            _pageIO.WritePage(_metadataPageId, buffer);
        }
        finally
        {
            BufferPool.Return(buffer);
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

        _collections.Add(entry);

        return entry;
    }

    public CollectionEntry? FindCollection(string name)
    {
        CollectionEntry? result = null;

        for (int i = 0; i < _collections.Count; i++)
        {
            if (_collections[i].Name == name)
            {
                result = _collections[i];
                break;
            }
        }

        return result;
    }

    public void UpdateCollection(CollectionEntry entry)
    {
        for (int i = 0; i < _collections.Count; i++)
        {
            if (_collections[i].Name == entry.Name)
            {
                _collections[i] = entry;
                break;
            }
        }
    }

    public void RemoveCollection(string name)
    {
        for (int i = 0; i < _collections.Count; i++)
        {
            if (_collections[i].Name == name)
            {
                _collections.RemoveAt(i);
                break;
            }
        }
    }

    public int GetCollectionCount()
    {
        return _collections.Count;
    }
}
