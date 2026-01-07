using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

public class HeaderPage
{
    public uint MagicNumber { get; set; }
    public int Version { get; set; }
    public int PageSize { get; set; }
    public int TotalPageCount { get; set; }
    public int BitmapStartPage { get; set; }
    public int BitmapPageCount { get; set; }
    public int FsmStartPage { get; set; }
    public int FsmPageCount { get; set; }
    public int CollectionsMetadataPage { get; set; }
    public byte MmapHint { get; set; }
    public int LastCommitFrame { get; set; }
    public ulong WalChecksum { get; set; }

    public byte[] Serialize(int pageSize)
    {
        byte[] buffer = new byte[pageSize];
        int offset = 0;

        BinaryHelper.WriteUInt32LE(buffer, offset, MagicNumber);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, Version);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, PageSize);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, TotalPageCount);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, BitmapStartPage);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, BitmapPageCount);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, FsmStartPage);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, FsmPageCount);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, CollectionsMetadataPage);
        offset += 4;

        buffer[offset] = MmapHint;
        offset += 1;

        BinaryHelper.WriteInt32LE(buffer, offset, LastCommitFrame);
        offset += 4;

        BinaryHelper.WriteUInt64LE(buffer, offset, WalChecksum);
        offset += 8;

        byte[] result = buffer;

        return result;
    }

    public static HeaderPage Deserialize(byte[] buffer)
    {
        HeaderPage header = new HeaderPage();
        int offset = 0;

        header.MagicNumber = BinaryHelper.ReadUInt32LE(buffer, offset);
        offset += 4;

        header.Version = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.PageSize = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.TotalPageCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.BitmapStartPage = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.BitmapPageCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.FsmStartPage = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.FsmPageCount = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.CollectionsMetadataPage = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.MmapHint = buffer[offset];
        offset += 1;

        header.LastCommitFrame = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.WalChecksum = BinaryHelper.ReadUInt64LE(buffer, offset);
        offset += 8;

        HeaderPage result = header;

        return result;
    }
}
