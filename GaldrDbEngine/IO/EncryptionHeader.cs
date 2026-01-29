using System;
using System.Security.Cryptography;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.IO;

internal sealed class EncryptionHeader
{
    public const int HEADER_SIZE = 32;
    public const int ENCRYPTION_VERSION = 1;
    public const int SALT_SIZE = 16;

    public uint Magic { get; set; }
    public int Version { get; set; }
    public int KdfIterations { get; set; }
    public byte[] Salt { get; set; }
    public int PageSize { get; set; }

    public EncryptionHeader()
    {
        Magic = PageConstants.ENCRYPTION_MAGIC;
        Version = ENCRYPTION_VERSION;
        Salt = new byte[SALT_SIZE];
    }

    public static EncryptionHeader Create(int kdfIterations)
    {
        EncryptionHeader header = new EncryptionHeader();
        header.KdfIterations = kdfIterations;
        RandomNumberGenerator.Fill(header.Salt);
        return header;
    }

    public void SerializeTo(Span<byte> buffer)
    {
        BinaryHelper.WriteUInt32LE(buffer, 0, Magic);
        BinaryHelper.WriteInt32LE(buffer, 4, Version);
        BinaryHelper.WriteInt32LE(buffer, 8, KdfIterations);
        Salt.AsSpan().CopyTo(buffer.Slice(12, SALT_SIZE));
        BinaryHelper.WriteInt32LE(buffer, 28, PageSize);
    }

    public static EncryptionHeader Deserialize(ReadOnlySpan<byte> buffer)
    {
        EncryptionHeader header = new EncryptionHeader();
        header.Magic = BinaryHelper.ReadUInt32LE(buffer, 0);
        header.Version = BinaryHelper.ReadInt32LE(buffer, 4);
        header.KdfIterations = BinaryHelper.ReadInt32LE(buffer, 8);
        buffer.Slice(12, SALT_SIZE).CopyTo(header.Salt);
        header.PageSize = BinaryHelper.ReadInt32LE(buffer, 28);
        return header;
    }

    public bool IsValid()
    {
        return Magic == PageConstants.ENCRYPTION_MAGIC && Version == ENCRYPTION_VERSION;
    }
}
