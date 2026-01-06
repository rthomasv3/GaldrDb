using System;
using System.Text;

namespace GaldrDbCore.Utilities;

public static class BinaryHelper
{
    private static readonly uint[] _crc32Table = GenerateCrc32Table();
    private static readonly ulong[] _crc64Table = GenerateCrc64Table();

    public static void WriteInt32LE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte)(value & 0xFF);
        buffer[offset + 1] = (byte)((value >> 8) & 0xFF);
        buffer[offset + 2] = (byte)((value >> 16) & 0xFF);
        buffer[offset + 3] = (byte)((value >> 24) & 0xFF);
    }

    public static int ReadInt32LE(byte[] buffer, int offset)
    {
        int result = buffer[offset] |
                    (buffer[offset + 1] << 8) |
                    (buffer[offset + 2] << 16) |
                    (buffer[offset + 3] << 24);

        return result;
    }

    public static void WriteUInt32LE(byte[] buffer, int offset, uint value)
    {
        buffer[offset] = (byte)(value & 0xFF);
        buffer[offset + 1] = (byte)((value >> 8) & 0xFF);
        buffer[offset + 2] = (byte)((value >> 16) & 0xFF);
        buffer[offset + 3] = (byte)((value >> 24) & 0xFF);
    }

    public static uint ReadUInt32LE(byte[] buffer, int offset)
    {
        uint result = buffer[offset] |
                     ((uint)buffer[offset + 1] << 8) |
                     ((uint)buffer[offset + 2] << 16) |
                     ((uint)buffer[offset + 3] << 24);

        return result;
    }

    public static void WriteUInt64LE(byte[] buffer, int offset, ulong value)
    {
        buffer[offset] = (byte)(value & 0xFF);
        buffer[offset + 1] = (byte)((value >> 8) & 0xFF);
        buffer[offset + 2] = (byte)((value >> 16) & 0xFF);
        buffer[offset + 3] = (byte)((value >> 24) & 0xFF);
        buffer[offset + 4] = (byte)((value >> 32) & 0xFF);
        buffer[offset + 5] = (byte)((value >> 40) & 0xFF);
        buffer[offset + 6] = (byte)((value >> 48) & 0xFF);
        buffer[offset + 7] = (byte)((value >> 56) & 0xFF);
    }

    public static ulong ReadUInt64LE(byte[] buffer, int offset)
    {
        ulong result = buffer[offset] |
                      ((ulong)buffer[offset + 1] << 8) |
                      ((ulong)buffer[offset + 2] << 16) |
                      ((ulong)buffer[offset + 3] << 24) |
                      ((ulong)buffer[offset + 4] << 32) |
                      ((ulong)buffer[offset + 5] << 40) |
                      ((ulong)buffer[offset + 6] << 48) |
                      ((ulong)buffer[offset + 7] << 56);

        return result;
    }

    public static void WriteUInt16LE(byte[] buffer, int offset, ushort value)
    {
        buffer[offset] = (byte)(value & 0xFF);
        buffer[offset + 1] = (byte)((value >> 8) & 0xFF);
    }

    public static ushort ReadUInt16LE(byte[] buffer, int offset)
    {
        ushort result = (ushort)(buffer[offset] | (buffer[offset + 1] << 8));

        return result;
    }

    public static int WriteString(byte[] buffer, int offset, string value)
    {
        byte[] stringBytes = Encoding.UTF8.GetBytes(value);
        int length = stringBytes.Length;

        WriteInt32LE(buffer, offset, length);
        Array.Copy(stringBytes, 0, buffer, offset + 4, length);

        int totalBytesWritten = 4 + length;

        return totalBytesWritten;
    }

    public static (string value, int bytesRead) ReadString(byte[] buffer, int offset)
    {
        int length = ReadInt32LE(buffer, offset);
        string value = Encoding.UTF8.GetString(buffer, offset + 4, length);
        int bytesRead = 4 + length;

        return (value, bytesRead);
    }

    public static uint CalculateCRC32(byte[] data)
    {
        uint crc = 0xFFFFFFFF;

        for (int i = 0; i < data.Length; i++)
        {
            byte index = (byte)((crc ^ data[i]) & 0xFF);
            crc = (crc >> 8) ^ _crc32Table[index];
        }

        uint result = crc ^ 0xFFFFFFFF;

        return result;
    }

    public static ulong CalculateCRC64(byte[] data)
    {
        ulong crc = 0xFFFFFFFFFFFFFFFF;

        for (int i = 0; i < data.Length; i++)
        {
            byte index = (byte)((crc ^ data[i]) & 0xFF);
            crc = (crc >> 8) ^ _crc64Table[index];
        }

        ulong result = crc ^ 0xFFFFFFFFFFFFFFFF;

        return result;
    }

    private static uint[] GenerateCrc32Table()
    {
        uint[] table = new uint[256];
        uint polynomial = 0xEDB88320;

        for (uint i = 0; i < 256; i++)
        {
            uint crc = i;

            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                {
                    crc = (crc >> 1) ^ polynomial;
                }
                else
                {
                    crc = crc >> 1;
                }
            }

            table[i] = crc;
        }

        return table;
    }

    private static ulong[] GenerateCrc64Table()
    {
        ulong[] table = new ulong[256];
        ulong polynomial = 0xC96C5795D7870F42;

        for (ulong i = 0; i < 256; i++)
        {
            ulong crc = i;

            for (int j = 0; j < 8; j++)
            {
                if ((crc & 1) != 0)
                {
                    crc = (crc >> 1) ^ polynomial;
                }
                else
                {
                    crc = crc >> 1;
                }
            }

            table[i] = crc;
        }

        return table;
    }
}
