using System;
using System.Collections.Generic;
using System.Text;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class CollectionEntry
{
    public string Name { get; set; }
    public int RootPage { get; set; }
    public int DocumentCount { get; set; }
    public int NextId { get; set; }
    public List<IndexDefinition> Indexes { get; set; }

    public CollectionEntry()
    {
        Indexes = new List<IndexDefinition>();
    }

    public int SerializeTo(byte[] buffer, int startOffset)
    {
        int offset = startOffset;
        int nameByteCount = Encoding.UTF8.GetByteCount(Name);

        BinaryHelper.WriteInt32LE(buffer, offset, nameByteCount);
        offset += 4;

        Encoding.UTF8.GetBytes(Name, 0, Name.Length, buffer, offset);
        offset += nameByteCount;

        BinaryHelper.WriteInt32LE(buffer, offset, RootPage);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, DocumentCount);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, NextId);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, Indexes.Count);
        offset += 4;

        for (int i = 0; i < Indexes.Count; i++)
        {
            offset += Indexes[i].SerializeTo(buffer, offset);
        }

        return offset - startOffset;
    }

    public static CollectionEntry Deserialize(byte[] buffer, int startOffset, out int bytesRead)
    {
        CollectionEntry entry = new CollectionEntry();
        int offset = startOffset;

        int nameLength = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        byte[] nameBytes = new byte[nameLength];
        Array.Copy(buffer, offset, nameBytes, 0, nameLength);
        entry.Name = Encoding.UTF8.GetString(nameBytes);
        offset += nameLength;

        entry.RootPage = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        entry.DocumentCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        entry.NextId = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        int indexCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        for (int i = 0; i < indexCount; i++)
        {
            int indexBytesRead = 0;
            IndexDefinition index = IndexDefinition.Deserialize(buffer, offset, out indexBytesRead);
            entry.Indexes.Add(index);
            offset += indexBytesRead;
        }

        bytesRead = offset - startOffset;

        return entry;
    }

    public int GetSerializedSize()
    {
        int nameByteCount = Encoding.UTF8.GetByteCount(Name);
        int result = 4 + nameByteCount + 4 + 4 + 4 + 4;

        for (int i = 0; i < Indexes.Count; i++)
        {
            result += Indexes[i].GetSerializedSize();
        }

        return result;
    }

    public IndexDefinition FindIndex(string fieldName)
    {
        IndexDefinition result = null;

        for (int i = 0; i < Indexes.Count; i++)
        {
            if (Indexes[i].FieldName == fieldName)
            {
                result = Indexes[i];
                break;
            }
        }

        return result;
    }
}
