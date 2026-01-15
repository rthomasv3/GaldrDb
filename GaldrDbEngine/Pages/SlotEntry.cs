using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

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
            size += 4 * PageCount;
        }

        int result = size;

        return result;
    }

    public void SerializeTo(byte[] buffer, int startOffset)
    {
        int offset = startOffset;

        BinaryHelper.WriteInt32LE(buffer, offset, PageCount);
        offset += 4;

        if (PageIds != null && PageCount > 0)
        {
            for (int i = 0; i < PageCount; i++)
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
    }

    public static SlotEntry Deserialize(byte[] buffer, int startOffset)
    {
        SlotEntry entry = new SlotEntry();
        int offset = startOffset;

        entry.PageCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        if (entry.PageCount > 0)
        {
            entry.PageIds = IntArrayPool.Rent(entry.PageCount);

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

    public void ReturnPageIdsToPool()
    {
        if (PageIds != null)
        {
            IntArrayPool.Return(PageIds);
            PageIds = null;
        }
    }
}
