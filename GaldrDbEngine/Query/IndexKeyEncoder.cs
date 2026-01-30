using System;
using System.Text;

namespace GaldrDbEngine.Query;

internal static class IndexKeyEncoder
{
    private const byte NULL_PREFIX = 0x00;
    private const byte VALUE_PREFIX = 0x01;

    /// <summary>
    /// The minimum key that represents a non-null value. Used as start key in range queries
    /// that should exclude null values (e.g., LessThan, LessThanOrEqual).
    /// </summary>
    public static readonly byte[] MinimumNonNullKey = new byte[] { VALUE_PREFIX };

    public static byte[] Encode(object value, GaldrFieldType fieldType)
    {
        byte[] result;

        if (value == null)
        {
            result = new byte[] { NULL_PREFIX };
        }
        else
        {
            byte[] valueBytes = fieldType switch
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
                GaldrFieldType.Byte => EncodeByte((byte)value),
                GaldrFieldType.SByte => EncodeSByte((sbyte)value),
                GaldrFieldType.Int16 => EncodeInt16((short)value),
                GaldrFieldType.UInt16 => EncodeUInt16((ushort)value),
                GaldrFieldType.UInt32 => EncodeUInt32((uint)value),
                GaldrFieldType.UInt64 => EncodeUInt64((ulong)value),
                GaldrFieldType.Single => EncodeSingle((float)value),
                GaldrFieldType.Char => EncodeChar((char)value),
                GaldrFieldType.TimeSpan => EncodeTimeSpan((TimeSpan)value),
                GaldrFieldType.DateOnly => EncodeDateOnly((DateOnly)value),
                GaldrFieldType.TimeOnly => EncodeTimeOnly((TimeOnly)value),
                GaldrFieldType.Complex => throw new NotSupportedException("Complex types cannot be encoded as index keys"),
                _ => throw new NotSupportedException($"Field type {fieldType} is not supported for index encoding")
            };

            result = new byte[valueBytes.Length + 1];
            result[0] = VALUE_PREFIX;
            Array.Copy(valueBytes, 0, result, 1, valueBytes.Length);
        }

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

    private static byte[] EncodeByte(byte value)
    {
        return new byte[] { value };
    }

    private static byte[] EncodeSByte(sbyte value)
    {
        return new byte[] { (byte)(value ^ sbyte.MinValue) };
    }

    private static byte[] EncodeInt16(short value)
    {
        ushort encoded = (ushort)(value ^ short.MinValue);
        return new byte[]
        {
            (byte)(encoded >> 8),
            (byte)encoded
        };
    }

    private static byte[] EncodeUInt16(ushort value)
    {
        return new byte[]
        {
            (byte)(value >> 8),
            (byte)value
        };
    }

    private static byte[] EncodeUInt32(uint value)
    {
        return new byte[]
        {
            (byte)(value >> 24),
            (byte)(value >> 16),
            (byte)(value >> 8),
            (byte)value
        };
    }

    private static byte[] EncodeUInt64(ulong value)
    {
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++)
        {
            bytes[i] = (byte)(value >> (56 - i * 8));
        }

        return bytes;
    }

    private static byte[] EncodeSingle(float value)
    {
        int bits = BitConverter.SingleToInt32Bits(value);
        uint encoded;
        if (bits < 0)
        {
            encoded = ((uint)bits) ^ 0xFFFFFFFFU;
        }
        else
        {
            encoded = (uint)bits ^ 0x80000000U;
        }

        return new byte[]
        {
            (byte)(encoded >> 24),
            (byte)(encoded >> 16),
            (byte)(encoded >> 8),
            (byte)encoded
        };
    }

    private static byte[] EncodeChar(char value)
    {
        ushort charValue = value;
        return new byte[]
        {
            (byte)(charValue >> 8),
            (byte)charValue
        };
    }

    private static byte[] EncodeTimeSpan(TimeSpan value)
    {
        return EncodeInt64(value.Ticks);
    }

    private static byte[] EncodeDateOnly(DateOnly value)
    {
        return EncodeInt32(value.DayNumber);
    }

    private static byte[] EncodeTimeOnly(TimeOnly value)
    {
        return EncodeInt64(value.Ticks);
    }

    public static byte[] EncodePrefixEnd(string prefix)
    {
        byte[] prefixBytes = Encoding.UTF8.GetBytes(prefix);
        byte[] endBytes = new byte[prefixBytes.Length];
        Array.Copy(prefixBytes, endBytes, prefixBytes.Length);

        byte[] result = null;
        for (int i = endBytes.Length - 1; i >= 0 && result == null; i--)
        {
            if (endBytes[i] < 0xFF)
            {
                endBytes[i]++;
                byte[] withPrefix = new byte[endBytes.Length + 1];
                withPrefix[0] = VALUE_PREFIX;
                Array.Copy(endBytes, 0, withPrefix, 1, endBytes.Length);
                result = withPrefix;
            }
        }

        return result;
    }

    /// <summary>
    /// Encodes multiple field values into a compound key.
    /// </summary>
    /// <param name="values">The values in field order (can contain nulls).</param>
    /// <param name="fieldTypes">The field types in order.</param>
    /// <returns>The encoded compound key bytes.</returns>
    public static byte[] EncodeCompound(object[] values, GaldrFieldType[] fieldTypes)
    {
        return EncodeCompound(values, fieldTypes, values.Length);
    }

    /// <summary>
    /// Encodes a subset of field values into a compound key.
    /// </summary>
    public static byte[] EncodeCompound(object[] values, GaldrFieldType[] fieldTypes, int count)
    {
        int totalSize = 0;
        for (int i = 0; i < count; i++)
        {
            totalSize += GetCompoundFieldSize(values[i], fieldTypes[i]);
        }

        byte[] result = new byte[totalSize];
        int offset = 0;

        for (int i = 0; i < count; i++)
        {
            offset += EncodeCompoundField(result, offset, values[i], fieldTypes[i]);
        }

        return result;
    }

    /// <summary>
    /// Encodes a prefix of field values into a compound key for prefix queries.
    /// </summary>
    /// <param name="values">The values for the prefix fields (can contain nulls).</param>
    /// <param name="fieldTypes">The field types for the prefix fields.</param>
    /// <returns>The encoded compound key prefix bytes.</returns>
    public static byte[] EncodeCompoundPrefix(object[] values, GaldrFieldType[] fieldTypes)
    {
        return EncodeCompound(values, fieldTypes);
    }

    private static int GetCompoundFieldSize(object value, GaldrFieldType fieldType)
    {
        int result;

        if (value == null)
        {
            result = 1;
        }
        else if (fieldType == GaldrFieldType.String)
        {
            byte[] stringBytes = Encoding.UTF8.GetBytes((string)value);
            result = 1 + 2 + stringBytes.Length;
        }
        else
        {
            result = 1 + GetFixedTypeSize(fieldType);
        }

        return result;
    }

    private static int EncodeCompoundField(byte[] buffer, int offset, object value, GaldrFieldType fieldType)
    {
        int bytesWritten;

        if (value == null)
        {
            buffer[offset] = NULL_PREFIX;
            bytesWritten = 1;
        }
        else if (fieldType == GaldrFieldType.String)
        {
            buffer[offset] = VALUE_PREFIX;
            byte[] stringBytes = Encoding.UTF8.GetBytes((string)value);
            buffer[offset + 1] = (byte)((stringBytes.Length >> 8) & 0xFF);
            buffer[offset + 2] = (byte)(stringBytes.Length & 0xFF);
            Array.Copy(stringBytes, 0, buffer, offset + 3, stringBytes.Length);
            bytesWritten = 1 + 2 + stringBytes.Length;
        }
        else
        {
            buffer[offset] = VALUE_PREFIX;
            byte[] valueBytes = EncodeValue(value, fieldType);
            Array.Copy(valueBytes, 0, buffer, offset + 1, valueBytes.Length);
            bytesWritten = 1 + valueBytes.Length;
        }

        return bytesWritten;
    }

    private static byte[] EncodeValue(object value, GaldrFieldType fieldType)
    {
        return fieldType switch
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
            GaldrFieldType.Byte => EncodeByte((byte)value),
            GaldrFieldType.SByte => EncodeSByte((sbyte)value),
            GaldrFieldType.Int16 => EncodeInt16((short)value),
            GaldrFieldType.UInt16 => EncodeUInt16((ushort)value),
            GaldrFieldType.UInt32 => EncodeUInt32((uint)value),
            GaldrFieldType.UInt64 => EncodeUInt64((ulong)value),
            GaldrFieldType.Single => EncodeSingle((float)value),
            GaldrFieldType.Char => EncodeChar((char)value),
            GaldrFieldType.TimeSpan => EncodeTimeSpan((TimeSpan)value),
            GaldrFieldType.DateOnly => EncodeDateOnly((DateOnly)value),
            GaldrFieldType.TimeOnly => EncodeTimeOnly((TimeOnly)value),
            _ => throw new NotSupportedException($"Field type {fieldType} is not supported for compound index encoding")
        };
    }

    private static int GetFixedTypeSize(GaldrFieldType fieldType)
    {
        return fieldType switch
        {
            GaldrFieldType.Int32 => 4,
            GaldrFieldType.Int64 => 8,
            GaldrFieldType.Double => 8,
            GaldrFieldType.Decimal => 16,
            GaldrFieldType.Boolean => 1,
            GaldrFieldType.DateTime => 8,
            GaldrFieldType.DateTimeOffset => 16,
            GaldrFieldType.Guid => 16,
            GaldrFieldType.Byte => 1,
            GaldrFieldType.SByte => 1,
            GaldrFieldType.Int16 => 2,
            GaldrFieldType.UInt16 => 2,
            GaldrFieldType.UInt32 => 4,
            GaldrFieldType.UInt64 => 8,
            GaldrFieldType.Single => 4,
            GaldrFieldType.Char => 2,
            GaldrFieldType.TimeSpan => 8,
            GaldrFieldType.DateOnly => 4,
            GaldrFieldType.TimeOnly => 8,
            _ => throw new NotSupportedException($"Field type {fieldType} is not a fixed-size type")
        };
    }
}
