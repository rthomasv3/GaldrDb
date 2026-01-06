using System;
using System.Text;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Storage;

public struct CollectionEntry
{
    public string Name { get; set; }
    public int RootPage { get; set; }
    public int DocumentCount { get; set; }
    public int NextId { get; set; }

    public byte[] Serialize()
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(Name);
        int totalSize = 4 + nameBytes.Length + 4 + 4 + 4;
        byte[] buffer = new byte[totalSize];
        int offset = 0;

        BinaryHelper.WriteInt32LE(buffer, offset, nameBytes.Length);
        offset += 4;

        Array.Copy(nameBytes, 0, buffer, offset, nameBytes.Length);
        offset += nameBytes.Length;

        BinaryHelper.WriteInt32LE(buffer, offset, RootPage);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, DocumentCount);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, NextId);
        offset += 4;

        return buffer;
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

        bytesRead = offset - startOffset;

        return entry;
    }

    public int GetSerializedSize()
    {
        int nameByteCount = Encoding.UTF8.GetByteCount(Name);
        // 4 bytes each for: nameLength (int32), RootPage (int32), DocumentCount (int32), NextId (int32)
        int result = 4 + nameByteCount + 4 + 4 + 4;

        return result;
    }
}
