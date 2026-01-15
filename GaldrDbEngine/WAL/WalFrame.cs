using System;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WalFrame
{
    public const int FRAME_HEADER_SIZE = 40;

    public long FrameNumber { get; set; }
    public ulong TxId { get; set; }
    public int PageId { get; set; }
    public byte PageType { get; set; }
    public WalFrameFlags Flags { get; set; }
    public int DataLength { get; set; }
    public uint Salt1 { get; set; }
    public uint Salt2 { get; set; }
    public uint Checksum { get; set; }
    public byte[] Data { get; set; }

    public WalFrame()
    {
        Data = Array.Empty<byte>();
    }

    public int SerializeTo(byte[] buffer)
    {
        int totalSize = FRAME_HEADER_SIZE + Data.Length;
        int offset = 0;

        BinaryHelper.WriteUInt64LE(buffer, offset, (ulong)FrameNumber);
        offset += 8;

        BinaryHelper.WriteUInt64LE(buffer, offset, TxId);
        offset += 8;

        BinaryHelper.WriteInt32LE(buffer, offset, PageId);
        offset += 4;

        buffer[offset] = PageType;
        offset += 1;

        buffer[offset] = (byte)Flags;
        offset += 1;

        // Reserved 2 bytes
        buffer[offset] = 0;
        buffer[offset + 1] = 0;
        offset += 2;

        BinaryHelper.WriteInt32LE(buffer, offset, Data.Length);
        offset += 4;

        BinaryHelper.WriteUInt32LE(buffer, offset, Salt1);
        offset += 4;

        BinaryHelper.WriteUInt32LE(buffer, offset, Salt2);
        offset += 4;

        // Checksum placeholder - will be calculated after data is written
        offset += 4;

        // Write data
        if (Data.Length > 0)
        {
            Array.Copy(Data, 0, buffer, offset, Data.Length);
        }

        // Calculate checksum over entire frame (excluding checksum field at bytes 36-39)
        Checksum = CalculateFrameChecksumInPlace(buffer, totalSize);
        BinaryHelper.WriteUInt32LE(buffer, FRAME_HEADER_SIZE - 4, Checksum);

        return totalSize;
    }

    public static WalFrame DeserializeHeader(byte[] headerBuffer)
    {
        WalFrame frame = new WalFrame();
        int offset = 0;

        frame.FrameNumber = (long)BinaryHelper.ReadUInt64LE(headerBuffer, offset);
        offset += 8;

        frame.TxId = BinaryHelper.ReadUInt64LE(headerBuffer, offset);
        offset += 8;

        frame.PageId = BinaryHelper.ReadInt32LE(headerBuffer, offset);
        offset += 4;

        frame.PageType = headerBuffer[offset];
        offset += 1;

        frame.Flags = (WalFrameFlags)headerBuffer[offset];
        offset += 1;

        // Skip reserved 2 bytes
        offset += 2;

        frame.DataLength = BinaryHelper.ReadInt32LE(headerBuffer, offset);
        offset += 4;

        frame.Salt1 = BinaryHelper.ReadUInt32LE(headerBuffer, offset);
        offset += 4;

        frame.Salt2 = BinaryHelper.ReadUInt32LE(headerBuffer, offset);
        offset += 4;

        frame.Checksum = BinaryHelper.ReadUInt32LE(headerBuffer, offset);

        return frame;
    }

    public static WalFrame Deserialize(byte[] buffer)
    {
        WalFrame frame = DeserializeHeader(buffer);

        if (frame.DataLength > 0 && buffer.Length >= FRAME_HEADER_SIZE + frame.DataLength)
        {
            frame.Data = new byte[frame.DataLength];
            Array.Copy(buffer, FRAME_HEADER_SIZE, frame.Data, 0, frame.DataLength);
        }
        else
        {
            frame.Data = Array.Empty<byte>();
        }

        return frame;
    }

    public bool ValidateChecksum(int pageSize)
    {
        int bufferSize = FRAME_HEADER_SIZE + pageSize;
        byte[] buffer = BufferPool.Rent(bufferSize);
        try
        {
            int totalSize = SerializeTo(buffer);
            uint calculatedChecksum = CalculateFrameChecksumInPlace(buffer, totalSize);

            return calculatedChecksum == Checksum;
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    public bool IsCommit()
    {
        return (Flags & WalFrameFlags.Commit) != 0;
    }

    public bool IsCheckpoint()
    {
        return (Flags & WalFrameFlags.Checkpoint) != 0;
    }

    private static uint CalculateFrameChecksumInPlace(byte[] buffer, int totalSize)
    {
        // Calculate checksum over frame header (excluding checksum field at bytes 36-39) + data
        // Segment 1: bytes 0-35 (header before checksum)
        // Segment 2: bytes 40+ (data after header)
        int headerBeforeChecksum = FRAME_HEADER_SIZE - 4; // 36 bytes
        int dataLength = totalSize - FRAME_HEADER_SIZE;

        uint checksum;
        if (dataLength > 0)
        {
            checksum = BinaryHelper.CalculateCRC32Segmented(
                buffer,
                0, headerBeforeChecksum,
                FRAME_HEADER_SIZE, dataLength);
        }
        else
        {
            checksum = BinaryHelper.CalculateCRC32(buffer, 0, headerBeforeChecksum);
        }

        return checksum;
    }

}
