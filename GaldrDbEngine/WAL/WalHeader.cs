using System;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.WAL;

public class WalHeader
{
    public const int HEADER_SIZE = 32;
    public const uint WAL_MAGIC_NUMBER = 0x47414C57; // "GALW" in ASCII

    public uint MagicNumber { get; set; }
    public int Version { get; set; }
    public int PageSize { get; set; }
    public ulong CheckpointTxId { get; set; }
    public uint Salt1 { get; set; }
    public uint Salt2 { get; set; }
    public uint Checksum { get; set; }

    public WalHeader()
    {
        MagicNumber = WAL_MAGIC_NUMBER;
        Version = 1;
    }

    public void SerializeTo(Span<byte> buffer)
    {
        int offset = 0;

        BinaryHelper.WriteUInt32LE(buffer, offset, MagicNumber);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, Version);
        offset += 4;

        BinaryHelper.WriteInt32LE(buffer, offset, PageSize);
        offset += 4;

        BinaryHelper.WriteUInt64LE(buffer, offset, CheckpointTxId);
        offset += 8;

        BinaryHelper.WriteUInt32LE(buffer, offset, Salt1);
        offset += 4;

        BinaryHelper.WriteUInt32LE(buffer, offset, Salt2);
        offset += 4;

        // Calculate checksum over header bytes (excluding checksum field itself)
        Checksum = BinaryHelper.CalculateCRC32(buffer.Slice(0, HEADER_SIZE - 4));

        BinaryHelper.WriteUInt32LE(buffer, offset, Checksum);
    }

    public static WalHeader Deserialize(byte[] buffer)
    {
        WalHeader header = new WalHeader();
        int offset = 0;

        header.MagicNumber = BinaryHelper.ReadUInt32LE(buffer, offset);
        offset += 4;

        header.Version = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.PageSize = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        header.CheckpointTxId = BinaryHelper.ReadUInt64LE(buffer, offset);
        offset += 8;

        header.Salt1 = BinaryHelper.ReadUInt32LE(buffer, offset);
        offset += 4;

        header.Salt2 = BinaryHelper.ReadUInt32LE(buffer, offset);
        offset += 4;

        header.Checksum = BinaryHelper.ReadUInt32LE(buffer, offset);

        return header;
    }

    public bool ValidateChecksum()
    {
        byte[] buffer = BufferPool.Rent(HEADER_SIZE);
        try
        {
            SerializeTo(buffer);
            uint calculatedChecksum = BinaryHelper.CalculateCRC32(buffer, 0, HEADER_SIZE - 4);

            return calculatedChecksum == Checksum;
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    public bool ValidateMagicNumber()
    {
        return MagicNumber == WAL_MAGIC_NUMBER;
    }
}
