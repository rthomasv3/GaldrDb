using System;
using System.Text;
using GaldrDbEngine.Query;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class IndexDefinition
{
    public string FieldName { get; set; }
    public GaldrFieldType FieldType { get; set; }
    public int RootPageId { get; set; }
    public bool IsUnique { get; set; }

    public int SerializeTo(byte[] buffer, int startOffset)
    {
        int offset = startOffset;
        int nameByteCount = Encoding.UTF8.GetByteCount(FieldName);

        BinaryHelper.WriteInt32LE(buffer, offset, nameByteCount);
        offset += 4;

        Encoding.UTF8.GetBytes(FieldName, 0, FieldName.Length, buffer, offset);
        offset += nameByteCount;

        buffer[offset] = (byte)FieldType;
        offset += 1;

        BinaryHelper.WriteInt32LE(buffer, offset, RootPageId);
        offset += 4;

        buffer[offset] = IsUnique ? (byte)1 : (byte)0;
        offset += 1;

        return offset - startOffset;
    }

    public static IndexDefinition Deserialize(byte[] buffer, int startOffset, out int bytesRead)
    {
        IndexDefinition index = new IndexDefinition();
        int offset = startOffset;

        int nameLength = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        byte[] nameBytes = new byte[nameLength];
        Array.Copy(buffer, offset, nameBytes, 0, nameLength);
        index.FieldName = Encoding.UTF8.GetString(nameBytes);
        offset += nameLength;

        index.FieldType = (GaldrFieldType)buffer[offset];
        offset += 1;

        index.RootPageId = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        index.IsUnique = buffer[offset] == 1;
        offset += 1;

        bytesRead = offset - startOffset;

        return index;
    }

    public int GetSerializedSize()
    {
        int nameByteCount = Encoding.UTF8.GetByteCount(FieldName);
        int result = 4 + nameByteCount + 1 + 4 + 1;

        return result;
    }
}
