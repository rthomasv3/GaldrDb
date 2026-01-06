using GaldrDbCore.Utilities;

namespace GaldrDbCore.Pages;

public struct SlotEntry
{
    public int PageCount { get; set; }
    public int[] PageIds { get; set; }
    public int TotalSize { get; set; }
    public int Offset { get; set; }
    public int Length { get; set; }

    public int GetSerializedSize()
    {
        int size = 4 + 4 + 4 + 4;

        if (PageIds != null)
        {
            size += 4 * PageIds.Length;
        }

        int result = size;

        return result;
    }

    public byte[] Serialize()
    {
        int serializedSize = GetSerializedSize();
        byte[] buffer = new byte[serializedSize];
        int offset = 0;

        BinaryHelper.WriteInt32LE(buffer, offset, PageCount);
        offset += 4;

        if (PageIds != null && PageIds.Length > 0)
        {
            for (int i = 0; i < PageIds.Length; i++)
            {
                BinaryHelper.WriteInt32LE(buffer, offset, PageIds[i]);
                offset += 4;
            }
        }

        BinaryHelper.WriteInt32LE(buffer, offset, TotalSize);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, Offset);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, Length);
        offset += 4;

        byte[] result = buffer;

        return result;
    }

    public static SlotEntry Deserialize(byte[] buffer, int startOffset)
    {
        SlotEntry entry = new SlotEntry();
        int offset = startOffset;

        entry.PageCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        if (entry.PageCount > 0)
        {
            entry.PageIds = new int[entry.PageCount];

            for (int i = 0; i < entry.PageCount; i++)
            {
                entry.PageIds[i] = BinaryHelper.ReadInt32LE(buffer, offset);
                offset += 4;
            }
        }

        entry.TotalSize = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        entry.Offset = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        entry.Length = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        SlotEntry result = entry;

        return result;
    }
}
