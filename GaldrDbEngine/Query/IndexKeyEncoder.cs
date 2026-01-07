using System;
using System.Text;

namespace GaldrDbCore.Query;

public static class IndexKeyEncoder
{
    public static byte[] Encode(object value, GaldrFieldType fieldType)
    {
        byte[] result = fieldType switch
        {
            GaldrFieldType.String => EncodeString((string)value),
            GaldrFieldType.Int32 => EncodeInt32((int)value),
            GaldrFieldType.Int64 => EncodeInt64((long)value),
            GaldrFieldType.Double => EncodeDouble((double)value),
            GaldrFieldType.Decimal => EncodeDecimal((decimal)value),
            GaldrFieldType.Boolean => EncodeBoolean((bool)value),
            GaldrFieldType.DateTime => EncodeDateTime((DateTime)value),
            GaldrFieldType.DateTimeOffset => EncodeDateTimeOffset((DateTimeOffset)value),
            GaldrFieldType.Guid => EncodeGuid((Guid)value),
            GaldrFieldType.Complex => throw new NotSupportedException("Complex types cannot be encoded as index keys"),
            _ => throw new NotSupportedException($"Field type {fieldType} is not supported for index encoding")
        };

        return result;
    }

    private static byte[] EncodeString(string value)
    {
        return Encoding.UTF8.GetBytes(value);
    }

    private static byte[] EncodeInt32(int value)
    {
        uint encoded = (uint)(value ^ int.MinValue);
        byte[] result = new byte[]
        {
            (byte)(encoded >> 24),
            (byte)(encoded >> 16),
            (byte)(encoded >> 8),
            (byte)encoded
        };

        return result;
    }

    private static byte[] EncodeInt64(long value)
    {
        ulong encoded = (ulong)(value ^ long.MinValue);
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++)
        {
            bytes[i] = (byte)(encoded >> (56 - i * 8));
        }

        return bytes;
    }

    private static byte[] EncodeDouble(double value)
    {
        long bits = BitConverter.DoubleToInt64Bits(value);
        ulong encoded;
        if (bits < 0)
        {
            encoded = ((ulong)bits) ^ 0xFFFFFFFFFFFFFFFFUL;
        }
        else
        {
            encoded = (ulong)bits ^ 0x8000000000000000UL;
        }

        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++)
        {
            bytes[i] = (byte)(encoded >> (56 - i * 8));
        }

        return bytes;
    }

    private static byte[] EncodeDecimal(decimal value)
    {
        int[] parts = decimal.GetBits(value);
        byte[] bytes = new byte[16];

        int sign = (parts[3] >> 31) & 1;
        int scale = (parts[3] >> 16) & 0x7F;

        bytes[0] = (byte)(sign == 0 ? 0x80 : 0x00);
        bytes[1] = (byte)scale;

        uint lo = (uint)parts[0];
        uint mid = (uint)parts[1];
        uint hi = (uint)parts[2];

        if (sign == 0)
        {
            bytes[2] = (byte)(hi >> 24);
            bytes[3] = (byte)(hi >> 16);
            bytes[4] = (byte)(hi >> 8);
            bytes[5] = (byte)hi;
            bytes[6] = (byte)(mid >> 24);
            bytes[7] = (byte)(mid >> 16);
            bytes[8] = (byte)(mid >> 8);
            bytes[9] = (byte)mid;
            bytes[10] = (byte)(lo >> 24);
            bytes[11] = (byte)(lo >> 16);
            bytes[12] = (byte)(lo >> 8);
            bytes[13] = (byte)lo;
        }
        else
        {
            ulong loMid = ((ulong)mid << 32) | lo;
            ulong complement = ~loMid + 1;
            uint compHi = ~hi;
            if (complement == 0)
            {
                compHi++;
            }

            bytes[2] = (byte)(compHi >> 24);
            bytes[3] = (byte)(compHi >> 16);
            bytes[4] = (byte)(compHi >> 8);
            bytes[5] = (byte)compHi;
            bytes[6] = (byte)(complement >> 56);
            bytes[7] = (byte)(complement >> 48);
            bytes[8] = (byte)(complement >> 40);
            bytes[9] = (byte)(complement >> 32);
            bytes[10] = (byte)(complement >> 24);
            bytes[11] = (byte)(complement >> 16);
            bytes[12] = (byte)(complement >> 8);
            bytes[13] = (byte)complement;
        }

        bytes[14] = 0;
        bytes[15] = 0;

        return bytes;
    }

    private static byte[] EncodeBoolean(bool value)
    {
        return new byte[] { value ? (byte)1 : (byte)0 };
    }

    private static byte[] EncodeDateTime(DateTime value)
    {
        return EncodeInt64(value.Ticks);
    }

    private static byte[] EncodeDateTimeOffset(DateTimeOffset value)
    {
        byte[] ticksBytes = EncodeInt64(value.UtcTicks);
        byte[] offsetBytes = EncodeInt64(value.Offset.Ticks);

        byte[] result = new byte[16];
        Array.Copy(ticksBytes, 0, result, 0, 8);
        Array.Copy(offsetBytes, 0, result, 8, 8);

        return result;
    }

    private static byte[] EncodeGuid(Guid value)
    {
        return value.ToByteArray();
    }

    public static byte[] EncodePrefixEnd(string prefix)
    {
        byte[] prefixBytes = Encoding.UTF8.GetBytes(prefix);
        byte[] endBytes = new byte[prefixBytes.Length];
        Array.Copy(prefixBytes, endBytes, prefixBytes.Length);

        for (int i = endBytes.Length - 1; i >= 0; i--)
        {
            if (endBytes[i] < 0xFF)
            {
                endBytes[i]++;
                return endBytes;
            }
        }

        return null;
    }
}
