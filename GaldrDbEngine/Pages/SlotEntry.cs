using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

internal struct SlotEntry
{
    // Base slot size: PageCount(4) + TotalSize(4) + Offset(4) + Length(4) = 16 bytes
    public const int BASE_SLOT_SIZE = 16;

    // Single-page slot size: BASE_SLOT_SIZE + one PageId(4) = 20 bytes
    // Used by CanFit and compaction logic since single-page documents are the common case
    public const int SINGLE_PAGE_SLOT_SIZE = 20;

    public int PageCount { get; set; }
    public int[] PageIds { get; set; }
    public int TotalSize { get; set; }
    public int Offset { get; set; }
    public int Length { get; set; }

    public int GetSerializedSize()
    {
        int size = BASE_SLOT_SIZE;

        if (PageIds != null)
        {
            size += 4 * PageCount;
        }

        return size;
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
