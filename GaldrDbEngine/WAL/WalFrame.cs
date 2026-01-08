using System;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WalFrame
{
    public const int FRAME_HEADER_SIZE = 32;

    public long FrameNumber { get; set; }
    public ulong TxId { get; set; }
    public int PageId { get; set; }
    public byte PageType { get; set; }
    public WalFrameFlags Flags { get; set; }
    public int DataLength { get; set; }
    public uint Checksum { get; set; }
    public byte[] Data { get; set; }

    public WalFrame()
    {
        Data = Array.Empty<byte>();
    }

    public byte[] Serialize()
    {
        int totalSize = FRAME_HEADER_SIZE + Data.Length;
        byte[] buffer = new byte[totalSize];
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

        // Checksum placeholder - will be calculated after data is written
        offset += 4;

        // Write data
        if (Data.Length > 0)
        {
            Array.Copy(Data, 0, buffer, offset, Data.Length);
        }

        // Calculate checksum over entire frame (excluding checksum field)
        Checksum = CalculateFrameChecksum(buffer);
        BinaryHelper.WriteUInt32LE(buffer, FRAME_HEADER_SIZE - 4, Checksum);

        return buffer;
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

    public bool ValidateChecksum()
    {
        byte[] buffer = Serialize();
        uint calculatedChecksum = CalculateFrameChecksum(buffer);

        bool result = calculatedChecksum == Checksum;

        return result;
    }

    public bool IsCommit()
    {
        bool result = (Flags & WalFrameFlags.Commit) != 0;

        return result;
    }

    public bool IsCheckpoint()
    {
        bool result = (Flags & WalFrameFlags.Checkpoint) != 0;

        return result;
    }

    private static uint CalculateFrameChecksum(byte[] buffer)
    {
        // Calculate checksum over frame header (excluding checksum field) + data
        int checksumDataSize = buffer.Length - 4;
        byte[] checksumData = new byte[checksumDataSize];

        // Copy header bytes before checksum field (28 bytes)
        Array.Copy(buffer, 0, checksumData, 0, FRAME_HEADER_SIZE - 4);

        // Copy data bytes
        if (buffer.Length > FRAME_HEADER_SIZE)
        {
            Array.Copy(buffer, FRAME_HEADER_SIZE, checksumData, FRAME_HEADER_SIZE - 4, buffer.Length - FRAME_HEADER_SIZE);
        }

        uint checksum = BinaryHelper.CalculateCRC32(checksumData);

        return checksum;
    }
}
