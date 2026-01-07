using System;
using System.Text;
using GaldrDbEngine.Query;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

public class IndexDefinition
{
    public string FieldName { get; set; }
    public GaldrFieldType FieldType { get; set; }
    public int RootPageId { get; set; }
    public bool IsUnique { get; set; }

    public byte[] Serialize()
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(FieldName);
        int totalSize = 4 + nameBytes.Length + 1 + 4 + 1;
        byte[] buffer = new byte[totalSize];
        int offset = 0;

        BinaryHelper.WriteInt32LE(buffer, offset, nameBytes.Length);
        offset += 4;

        Array.Copy(nameBytes, 0, buffer, offset, nameBytes.Length);
        offset += nameBytes.Length;

        buffer[offset] = (byte)FieldType;
        offset += 1;

        BinaryHelper.WriteInt32LE(buffer, offset, RootPageId);
        offset += 4;

        buffer[offset] = IsUnique ? (byte)1 : (byte)0;
        offset += 1;

        return buffer;
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
