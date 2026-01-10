using System;
using System.Collections.Generic;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

public class DocumentPage
{
    private const int HEADER_SIZE = 13;

    public byte PageType { get; set; }
    public byte Flags { get; set; }
    public ushort SlotCount { get; set; }
    public ushort FreeSpaceOffset { get; set; }
    public ushort FreeSpaceEnd { get; set; }
    public uint Crc { get; set; }
    public List<SlotEntry> Slots { get; set; }
    public byte[] PageData { get; set; }

    private int _pageSize;

    public static DocumentPage CreateNew(int pageSize)
    {
        DocumentPage page = new DocumentPage
        {
            PageType = PageConstants.PAGE_TYPE_DOCUMENT,
            Flags = 0,
            SlotCount = 0,
            FreeSpaceOffset = (ushort)HEADER_SIZE,
            FreeSpaceEnd = (ushort)pageSize,
            Crc = 0,
            Slots = new List<SlotEntry>(),
            PageData = new byte[pageSize],
            _pageSize = pageSize
        };

        DocumentPage result = page;

        return result;
    }

    public bool CanFit(int dataSize)
    {
        int slotSize = 4 + 4 + 4 + 4;
        int requiredSpace = slotSize + dataSize;
        int availableSpace = FreeSpaceEnd - FreeSpaceOffset;
        bool result = requiredSpace <= availableSpace;

        return result;
    }

    public int AddDocument(ReadOnlySpan<byte> documentData, int[] pageIds, int totalSize)
    {
        int slotIndex = SlotCount;
        int dataLength = documentData.Length;

        SlotEntry entry = new SlotEntry
        {
            PageCount = pageIds.Length,
            PageIds = pageIds,
            TotalSize = totalSize,
            Offset = FreeSpaceEnd - dataLength,
            Length = dataLength
        };

        Slots.Add(entry);
        SlotCount++;

        documentData.CopyTo(PageData.AsSpan(entry.Offset, dataLength));

        FreeSpaceEnd = (ushort)(entry.Offset);
        FreeSpaceOffset = (ushort)(FreeSpaceOffset + entry.GetSerializedSize());

        int result = slotIndex;

        return result;
    }

    public byte[] GetDocumentData(int slotIndex)
    {
        if (slotIndex < 0 || slotIndex >= Slots.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(slotIndex));
        }

        SlotEntry entry = Slots[slotIndex];
        byte[] documentData = new byte[entry.Length];

        Array.Copy(PageData, entry.Offset, documentData, 0, entry.Length);

        byte[] result = documentData;

        return result;
    }

    public int GetFreeSpaceBytes()
    {
        int freeSpace = FreeSpaceEnd - FreeSpaceOffset;
        int result = freeSpace;

        return result;
    }

    public void SerializeTo(byte[] buffer)
    {
        // Clear the buffer first to ensure clean state
        Array.Clear(buffer, 0, _pageSize);

        int offset = 0;

        buffer[offset] = PageType;
        offset += 1;

        buffer[offset] = Flags;
        offset += 1;

        BinaryHelper.WriteUInt16LE(buffer, offset, SlotCount);
        offset += 2;

        BinaryHelper.WriteUInt16LE(buffer, offset, FreeSpaceOffset);
        offset += 2;

        BinaryHelper.WriteUInt16LE(buffer, offset, FreeSpaceEnd);
        offset += 2;

        BinaryHelper.WriteUInt32LE(buffer, offset, Crc);
        offset += 4;

        for (int i = 0; i < Slots.Count; i++)
        {
            Slots[i].SerializeTo(buffer, offset);
            offset += Slots[i].GetSerializedSize();
        }

        if (PageData != null)
        {
            Array.Copy(PageData, FreeSpaceEnd, buffer, FreeSpaceEnd, _pageSize - FreeSpaceEnd);
        }
    }

    public static void DeserializeTo(byte[] buffer, DocumentPage page, int pageSize)
    {
        page._pageSize = pageSize;

        // Ensure PageData is allocated and correct size
        if (page.PageData == null || page.PageData.Length < pageSize)
        {
            page.PageData = new byte[pageSize];
        }

        // Ensure Slots list exists
        if (page.Slots == null)
        {
            page.Slots = new List<SlotEntry>();
        }
        else
        {
            page.Slots.Clear();
        }

        int offset = 0;

        page.PageType = buffer[offset];
        offset += 1;

        page.Flags = buffer[offset];
        offset += 1;

        page.SlotCount = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        page.FreeSpaceOffset = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        page.FreeSpaceEnd = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        page.Crc = BinaryHelper.ReadUInt32LE(buffer, offset);
        offset += 4;

        for (int i = 0; i < page.SlotCount; i++)
        {
            SlotEntry entry = SlotEntry.Deserialize(buffer, offset);
            page.Slots.Add(entry);
            offset += entry.GetSerializedSize();
        }

        Array.Copy(buffer, 0, page.PageData, 0, pageSize);
    }

    public void Reset(int pageSize)
    {
        _pageSize = pageSize;
        PageType = PageConstants.PAGE_TYPE_DOCUMENT;
        Flags = 0;
        SlotCount = 0;
        FreeSpaceOffset = (ushort)HEADER_SIZE;
        FreeSpaceEnd = (ushort)pageSize;
        Crc = 0;

        if (Slots == null)
        {
            Slots = new List<SlotEntry>();
        }
        else
        {
            Slots.Clear();
        }

        if (PageData == null || PageData.Length < pageSize)
        {
            PageData = new byte[pageSize];
        }
        else
        {
            Array.Clear(PageData, 0, pageSize);
        }
    }
}
