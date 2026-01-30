using System;
using System.Runtime.CompilerServices;
using System.Text;

namespace GaldrDbEngine.Query;

internal static class IndexKeyEncoder
{
    public const int COMPOUND_NULL_SIZE = 1;
    
    private const int COMPOUND_INT32_SIZE = 5;
    private const int COMPOUND_INT64_SIZE = 9;
    private const int COMPOUND_DOUBLE_SIZE = 9;
    private const int COMPOUND_DECIMAL_SIZE = 17;
    private const int COMPOUND_BOOLEAN_SIZE = 2;
    private const int COMPOUND_DATETIME_SIZE = 9;
    private const int COMPOUND_DATETIMEOFFSET_SIZE = 17;
    private const int COMPOUND_GUID_SIZE = 17;
    private const int COMPOUND_BYTE_SIZE = 2;
    private const int COMPOUND_SBYTE_SIZE = 2;
    private const int COMPOUND_INT16_SIZE = 3;
    private const int COMPOUND_UINT16_SIZE = 3;
    private const int COMPOUND_UINT32_SIZE = 5;
    private const int COMPOUND_UINT64_SIZE = 9;
    private const int COMPOUND_SINGLE_SIZE = 5;
    private const int COMPOUND_CHAR_SIZE = 3;
    private const int COMPOUND_TIMESPAN_SIZE = 9;
    private const int COMPOUND_DATEONLY_SIZE = 5;
    private const int COMPOUND_TIMEONLY_SIZE = 9;
    
    private const byte NULL_PREFIX = 0x00;
    private const byte VALUE_PREFIX = 0x01;
    private const byte NULL_ESCAPE = 0xFF;
    private const byte STRING_TERMINATOR = 0x00;

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
        byte[] utf8 = Encoding.UTF8.GetBytes(value);
        int nullCount = CountNullBytes(utf8);
        byte[] result = new byte[utf8.Length + nullCount + 1];

        int writeIndex = 0;
        for (int i = 0; i < utf8.Length; i++)
        {
            result[writeIndex] = utf8[i];
            writeIndex++;
            if (utf8[i] == 0x00)
            {
                result[writeIndex] = NULL_ESCAPE;
                writeIndex++;
            }
        }
        result[writeIndex] = STRING_TERMINATOR;

        return result;
    }

    private static int CountNullBytes(byte[] bytes)
    {
        int count = 0;
        for (int i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] == 0x00)
            {
                count++;
            }
        }
        return count;
    }

    private static int GetEscapedStringSize(string value)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(value);
        int nullCount = CountNullBytes(utf8);
        return utf8.Length + nullCount + 1;
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

    /// <summary>
    /// Encodes a string prefix for prefix search operations (StartsWith).
    /// Returns encoded bytes WITHOUT the null terminator, so it can match
    /// the beginning of any stored key that starts with this prefix.
    /// </summary>
    public static byte[] EncodePrefix(string prefix)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(prefix);
        int nullCount = CountNullBytes(utf8);
        byte[] result = new byte[1 + utf8.Length + nullCount];

        result[0] = VALUE_PREFIX;
        int writeIndex = 1;
        for (int i = 0; i < utf8.Length; i++)
        {
            result[writeIndex] = utf8[i];
            writeIndex++;
            if (utf8[i] == 0x00)
            {
                result[writeIndex] = NULL_ESCAPE;
                writeIndex++;
            }
        }

        return result;
    }

    /// <summary>
    /// Gets the size needed to encode a string prefix in compound key format.
    /// This is for StartsWith queries and excludes the null terminator.
    /// </summary>
    public static int GetCompoundPrefixSize(string prefix)
    {
        int result;
        if (prefix == null)
        {
            result = 1;
        }
        else
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(prefix);
            int nullCount = CountNullBytes(utf8);
            result = 1 + utf8.Length + nullCount;
        }
        return result;
    }

    /// <summary>
    /// Encodes a string prefix directly to a buffer in compound key format.
    /// This is for StartsWith queries and excludes the null terminator.
    /// </summary>
    public static int EncodeCompoundPrefix(byte[] buffer, int offset, string prefix)
    {
        int bytesWritten;
        if (prefix == null)
        {
            buffer[offset] = NULL_PREFIX;
            bytesWritten = 1;
        }
        else
        {
            buffer[offset] = VALUE_PREFIX;
            byte[] utf8 = Encoding.UTF8.GetBytes(prefix);
            int writeIndex = offset + 1;
            for (int i = 0; i < utf8.Length; i++)
            {
                buffer[writeIndex] = utf8[i];
                writeIndex++;
                if (utf8[i] == 0x00)
                {
                    buffer[writeIndex] = NULL_ESCAPE;
                    writeIndex++;
                }
            }
            bytesWritten = writeIndex - offset;
        }
        return bytesWritten;
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
                int nullCount = CountNullBytes(endBytes);
                byte[] withPrefix = new byte[1 + endBytes.Length + nullCount + 1];
                withPrefix[0] = VALUE_PREFIX;
                int writeIndex = 1;
                for (int j = 0; j < endBytes.Length; j++)
                {
                    withPrefix[writeIndex] = endBytes[j];
                    writeIndex++;
                    if (endBytes[j] == 0x00)
                    {
                        withPrefix[writeIndex] = NULL_ESCAPE;
                        writeIndex++;
                    }
                }
                withPrefix[writeIndex] = STRING_TERMINATOR;
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
            result = 1 + GetEscapedStringSize((string)value);
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
            bytesWritten = EncodeCompoundString(buffer, offset, (string)value);
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

    public static int GetCompoundStringSize(string value)
    {
        int result;
        if (value == null)
        {
            result = 1;
        }
        else
        {
            result = 1 + GetEscapedStringSize(value);
        }
        return result;
    }

    public static int EncodeCompoundNull(byte[] buffer, int offset)
    {
        buffer[offset] = NULL_PREFIX;
        return 1;
    }

    public static int EncodeCompoundString(byte[] buffer, int offset, string value)
    {
        int bytesWritten;
        if (value == null)
        {
            buffer[offset] = NULL_PREFIX;
            bytesWritten = 1;
        }
        else
        {
            buffer[offset] = VALUE_PREFIX;
            byte[] utf8 = Encoding.UTF8.GetBytes(value);
            int writeIndex = offset + 1;
            for (int i = 0; i < utf8.Length; i++)
            {
                buffer[writeIndex] = utf8[i];
                writeIndex++;
                if (utf8[i] == 0x00)
                {
                    buffer[writeIndex] = NULL_ESCAPE;
                    writeIndex++;
                }
            }
            buffer[writeIndex] = STRING_TERMINATOR;
            writeIndex++;
            bytesWritten = writeIndex - offset;
        }
        return bytesWritten;
    }

    public static int EncodeCompoundInt32(byte[] buffer, int offset, int value)
    {
        buffer[offset] = VALUE_PREFIX;
        uint encoded = (uint)(value ^ int.MinValue);
        buffer[offset + 1] = (byte)(encoded >> 24);
        buffer[offset + 2] = (byte)(encoded >> 16);
        buffer[offset + 3] = (byte)(encoded >> 8);
        buffer[offset + 4] = (byte)encoded;
        return 5;
    }

    public static int EncodeCompoundInt64(byte[] buffer, int offset, long value)
    {
        buffer[offset] = VALUE_PREFIX;
        ulong encoded = (ulong)(value ^ long.MinValue);
        for (int i = 0; i < 8; i++)
        {
            buffer[offset + 1 + i] = (byte)(encoded >> (56 - i * 8));
        }
        return 9;
    }

    public static int EncodeCompoundDouble(byte[] buffer, int offset, double value)
    {
        buffer[offset] = VALUE_PREFIX;
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
        for (int i = 0; i < 8; i++)
        {
            buffer[offset + 1 + i] = (byte)(encoded >> (56 - i * 8));
        }
        return 9;
    }

    public static int EncodeCompoundDecimal(byte[] buffer, int offset, decimal value)
    {
        buffer[offset] = VALUE_PREFIX;
        int[] parts = decimal.GetBits(value);
        int sign = (parts[3] >> 31) & 1;
        int scale = (parts[3] >> 16) & 0x7F;

        buffer[offset + 1] = (byte)(sign == 0 ? 0x80 : 0x00);
        buffer[offset + 2] = (byte)scale;

        uint lo = (uint)parts[0];
        uint mid = (uint)parts[1];
        uint hi = (uint)parts[2];

        if (sign == 0)
        {
            buffer[offset + 3] = (byte)(hi >> 24);
            buffer[offset + 4] = (byte)(hi >> 16);
            buffer[offset + 5] = (byte)(hi >> 8);
            buffer[offset + 6] = (byte)hi;
            buffer[offset + 7] = (byte)(mid >> 24);
            buffer[offset + 8] = (byte)(mid >> 16);
            buffer[offset + 9] = (byte)(mid >> 8);
            buffer[offset + 10] = (byte)mid;
            buffer[offset + 11] = (byte)(lo >> 24);
            buffer[offset + 12] = (byte)(lo >> 16);
            buffer[offset + 13] = (byte)(lo >> 8);
            buffer[offset + 14] = (byte)lo;
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

            buffer[offset + 3] = (byte)(compHi >> 24);
            buffer[offset + 4] = (byte)(compHi >> 16);
            buffer[offset + 5] = (byte)(compHi >> 8);
            buffer[offset + 6] = (byte)compHi;
            buffer[offset + 7] = (byte)(complement >> 56);
            buffer[offset + 8] = (byte)(complement >> 48);
            buffer[offset + 9] = (byte)(complement >> 40);
            buffer[offset + 10] = (byte)(complement >> 32);
            buffer[offset + 11] = (byte)(complement >> 24);
            buffer[offset + 12] = (byte)(complement >> 16);
            buffer[offset + 13] = (byte)(complement >> 8);
            buffer[offset + 14] = (byte)complement;
        }

        buffer[offset + 15] = 0;
        buffer[offset + 16] = 0;
        return 17;
    }

    public static int EncodeCompoundBoolean(byte[] buffer, int offset, bool value)
    {
        buffer[offset] = VALUE_PREFIX;
        buffer[offset + 1] = value ? (byte)1 : (byte)0;
        return 2;
    }

    public static int EncodeCompoundDateTime(byte[] buffer, int offset, DateTime value)
    {
        return EncodeCompoundInt64(buffer, offset, value.Ticks);
    }

    public static int EncodeCompoundDateTimeOffset(byte[] buffer, int offset, DateTimeOffset value)
    {
        buffer[offset] = VALUE_PREFIX;
        ulong utcEncoded = (ulong)(value.UtcTicks ^ long.MinValue);
        for (int i = 0; i < 8; i++)
        {
            buffer[offset + 1 + i] = (byte)(utcEncoded >> (56 - i * 8));
        }
        ulong offsetEncoded = (ulong)(value.Offset.Ticks ^ long.MinValue);
        for (int i = 0; i < 8; i++)
        {
            buffer[offset + 9 + i] = (byte)(offsetEncoded >> (56 - i * 8));
        }
        return 17;
    }

    public static int EncodeCompoundGuid(byte[] buffer, int offset, Guid value)
    {
        buffer[offset] = VALUE_PREFIX;
        byte[] guidBytes = value.ToByteArray();
        Array.Copy(guidBytes, 0, buffer, offset + 1, 16);
        return 17;
    }

    public static int EncodeCompoundByte(byte[] buffer, int offset, byte value)
    {
        buffer[offset] = VALUE_PREFIX;
        buffer[offset + 1] = value;
        return 2;
    }

    public static int EncodeCompoundSByte(byte[] buffer, int offset, sbyte value)
    {
        buffer[offset] = VALUE_PREFIX;
        buffer[offset + 1] = (byte)(value ^ sbyte.MinValue);
        return 2;
    }

    public static int EncodeCompoundInt16(byte[] buffer, int offset, short value)
    {
        buffer[offset] = VALUE_PREFIX;
        ushort encoded = (ushort)(value ^ short.MinValue);
        buffer[offset + 1] = (byte)(encoded >> 8);
        buffer[offset + 2] = (byte)encoded;
        return 3;
    }

    public static int EncodeCompoundUInt16(byte[] buffer, int offset, ushort value)
    {
        buffer[offset] = VALUE_PREFIX;
        buffer[offset + 1] = (byte)(value >> 8);
        buffer[offset + 2] = (byte)value;
        return 3;
    }

    public static int EncodeCompoundUInt32(byte[] buffer, int offset, uint value)
    {
        buffer[offset] = VALUE_PREFIX;
        buffer[offset + 1] = (byte)(value >> 24);
        buffer[offset + 2] = (byte)(value >> 16);
        buffer[offset + 3] = (byte)(value >> 8);
        buffer[offset + 4] = (byte)value;
        return 5;
    }

    public static int EncodeCompoundUInt64(byte[] buffer, int offset, ulong value)
    {
        buffer[offset] = VALUE_PREFIX;
        for (int i = 0; i < 8; i++)
        {
            buffer[offset + 1 + i] = (byte)(value >> (56 - i * 8));
        }
        return 9;
    }

    public static int EncodeCompoundSingle(byte[] buffer, int offset, float value)
    {
        buffer[offset] = VALUE_PREFIX;
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
        buffer[offset + 1] = (byte)(encoded >> 24);
        buffer[offset + 2] = (byte)(encoded >> 16);
        buffer[offset + 3] = (byte)(encoded >> 8);
        buffer[offset + 4] = (byte)encoded;
        return 5;
    }

    public static int EncodeCompoundChar(byte[] buffer, int offset, char value)
    {
        buffer[offset] = VALUE_PREFIX;
        ushort charValue = value;
        buffer[offset + 1] = (byte)(charValue >> 8);
        buffer[offset + 2] = (byte)charValue;
        return 3;
    }

    public static int EncodeCompoundTimeSpan(byte[] buffer, int offset, TimeSpan value)
    {
        return EncodeCompoundInt64(buffer, offset, value.Ticks);
    }

    public static int EncodeCompoundDateOnly(byte[] buffer, int offset, DateOnly value)
    {
        return EncodeCompoundInt32(buffer, offset, value.DayNumber);
    }

    public static int EncodeCompoundTimeOnly(byte[] buffer, int offset, TimeOnly value)
    {
        return EncodeCompoundInt64(buffer, offset, value.Ticks);
    }

    /// <summary>
    /// Gets the compound-encoded size for a value of type T without boxing.
    /// Uses typeof(T) checks which are compile-time constants in AOT.
    /// </summary>
    public static int GetCompoundEncodedSize<T>(T value)
    {
        int result;
        if (value == null)
        {
            result = COMPOUND_NULL_SIZE;
        }
        else if (typeof(T) == typeof(string))
        {
            result = GetCompoundStringSize(Unsafe.As<T, string>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(int))
        {
            result = COMPOUND_INT32_SIZE;
        }
        else if (typeof(T) == typeof(long))
        {
            result = COMPOUND_INT64_SIZE;
        }
        else if (typeof(T) == typeof(double))
        {
            result = COMPOUND_DOUBLE_SIZE;
        }
        else if (typeof(T) == typeof(decimal))
        {
            result = COMPOUND_DECIMAL_SIZE;
        }
        else if (typeof(T) == typeof(bool))
        {
            result = COMPOUND_BOOLEAN_SIZE;
        }
        else if (typeof(T) == typeof(DateTime))
        {
            result = COMPOUND_DATETIME_SIZE;
        }
        else if (typeof(T) == typeof(DateTimeOffset))
        {
            result = COMPOUND_DATETIMEOFFSET_SIZE;
        }
        else if (typeof(T) == typeof(Guid))
        {
            result = COMPOUND_GUID_SIZE;
        }
        else if (typeof(T) == typeof(byte))
        {
            result = COMPOUND_BYTE_SIZE;
        }
        else if (typeof(T) == typeof(sbyte))
        {
            result = COMPOUND_SBYTE_SIZE;
        }
        else if (typeof(T) == typeof(short))
        {
            result = COMPOUND_INT16_SIZE;
        }
        else if (typeof(T) == typeof(ushort))
        {
            result = COMPOUND_UINT16_SIZE;
        }
        else if (typeof(T) == typeof(uint))
        {
            result = COMPOUND_UINT32_SIZE;
        }
        else if (typeof(T) == typeof(ulong))
        {
            result = COMPOUND_UINT64_SIZE;
        }
        else if (typeof(T) == typeof(float))
        {
            result = COMPOUND_SINGLE_SIZE;
        }
        else if (typeof(T) == typeof(char))
        {
            result = COMPOUND_CHAR_SIZE;
        }
        else if (typeof(T) == typeof(TimeSpan))
        {
            result = COMPOUND_TIMESPAN_SIZE;
        }
        else if (typeof(T) == typeof(DateOnly))
        {
            result = COMPOUND_DATEONLY_SIZE;
        }
        else if (typeof(T) == typeof(TimeOnly))
        {
            result = COMPOUND_TIMEONLY_SIZE;
        }
        else
        {
            throw new NotSupportedException($"Type {typeof(T)} is not supported for compound encoding");
        }
        return result;
    }

    /// <summary>
    /// Encodes a value of type T directly to a buffer in compound format without boxing.
    /// Uses typeof(T) checks which are compile-time constants in AOT.
    /// </summary>
    public static int EncodeCompoundFieldTo<T>(byte[] buffer, int offset, T value)
    {
        int result;
        if (value == null)
        {
            result = EncodeCompoundNull(buffer, offset);
        }
        else if (typeof(T) == typeof(string))
        {
            result = EncodeCompoundString(buffer, offset, Unsafe.As<T, string>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(int))
        {
            result = EncodeCompoundInt32(buffer, offset, Unsafe.As<T, int>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(long))
        {
            result = EncodeCompoundInt64(buffer, offset, Unsafe.As<T, long>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(double))
        {
            result = EncodeCompoundDouble(buffer, offset, Unsafe.As<T, double>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(decimal))
        {
            result = EncodeCompoundDecimal(buffer, offset, Unsafe.As<T, decimal>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(bool))
        {
            result = EncodeCompoundBoolean(buffer, offset, Unsafe.As<T, bool>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(DateTime))
        {
            result = EncodeCompoundDateTime(buffer, offset, Unsafe.As<T, DateTime>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(DateTimeOffset))
        {
            result = EncodeCompoundDateTimeOffset(buffer, offset, Unsafe.As<T, DateTimeOffset>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(Guid))
        {
            result = EncodeCompoundGuid(buffer, offset, Unsafe.As<T, Guid>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(byte))
        {
            result = EncodeCompoundByte(buffer, offset, Unsafe.As<T, byte>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(sbyte))
        {
            result = EncodeCompoundSByte(buffer, offset, Unsafe.As<T, sbyte>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(short))
        {
            result = EncodeCompoundInt16(buffer, offset, Unsafe.As<T, short>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(ushort))
        {
            result = EncodeCompoundUInt16(buffer, offset, Unsafe.As<T, ushort>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(uint))
        {
            result = EncodeCompoundUInt32(buffer, offset, Unsafe.As<T, uint>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(ulong))
        {
            result = EncodeCompoundUInt64(buffer, offset, Unsafe.As<T, ulong>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(float))
        {
            result = EncodeCompoundSingle(buffer, offset, Unsafe.As<T, float>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(char))
        {
            result = EncodeCompoundChar(buffer, offset, Unsafe.As<T, char>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(TimeSpan))
        {
            result = EncodeCompoundTimeSpan(buffer, offset, Unsafe.As<T, TimeSpan>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(DateOnly))
        {
            result = EncodeCompoundDateOnly(buffer, offset, Unsafe.As<T, DateOnly>(ref Unsafe.AsRef(in value)));
        }
        else if (typeof(T) == typeof(TimeOnly))
        {
            result = EncodeCompoundTimeOnly(buffer, offset, Unsafe.As<T, TimeOnly>(ref Unsafe.AsRef(in value)));
        }
        else
        {
            throw new NotSupportedException($"Type {typeof(T)} is not supported for compound encoding");
        }
        return result;
    }

    /// <summary>
    /// Gets the compound-encoded size for a boxed value using the field type to dispatch.
    /// For use by dynamic filters that already store values as object.
    /// </summary>
    public static int GetCompoundEncodedSizeBoxed(object value, GaldrFieldType fieldType)
    {
        int result;
        if (value == null)
        {
            result = COMPOUND_NULL_SIZE;
        }
        else
        {
            result = fieldType switch
            {
                GaldrFieldType.String => GetCompoundStringSize((string)value),
                GaldrFieldType.Int32 => COMPOUND_INT32_SIZE,
                GaldrFieldType.Int64 => COMPOUND_INT64_SIZE,
                GaldrFieldType.Double => COMPOUND_DOUBLE_SIZE,
                GaldrFieldType.Decimal => COMPOUND_DECIMAL_SIZE,
                GaldrFieldType.Boolean => COMPOUND_BOOLEAN_SIZE,
                GaldrFieldType.DateTime => COMPOUND_DATETIME_SIZE,
                GaldrFieldType.DateTimeOffset => COMPOUND_DATETIMEOFFSET_SIZE,
                GaldrFieldType.Guid => COMPOUND_GUID_SIZE,
                GaldrFieldType.Byte => COMPOUND_BYTE_SIZE,
                GaldrFieldType.SByte => COMPOUND_SBYTE_SIZE,
                GaldrFieldType.Int16 => COMPOUND_INT16_SIZE,
                GaldrFieldType.UInt16 => COMPOUND_UINT16_SIZE,
                GaldrFieldType.UInt32 => COMPOUND_UINT32_SIZE,
                GaldrFieldType.UInt64 => COMPOUND_UINT64_SIZE,
                GaldrFieldType.Single => COMPOUND_SINGLE_SIZE,
                GaldrFieldType.Char => COMPOUND_CHAR_SIZE,
                GaldrFieldType.TimeSpan => COMPOUND_TIMESPAN_SIZE,
                GaldrFieldType.DateOnly => COMPOUND_DATEONLY_SIZE,
                GaldrFieldType.TimeOnly => COMPOUND_TIMEONLY_SIZE,
                _ => throw new NotSupportedException($"Field type {fieldType} is not supported for compound encoding")
            };
        }
        return result;
    }

    /// <summary>
    /// Encodes a boxed value to a buffer in compound format using the field type to dispatch.
    /// For use by dynamic filters that already store values as object.
    /// </summary>
    public static int EncodeCompoundFieldToBoxed(byte[] buffer, int offset, object value, GaldrFieldType fieldType)
    {
        int result;
        if (value == null)
        {
            result = EncodeCompoundNull(buffer, offset);
        }
        else
        {
            result = fieldType switch
            {
                GaldrFieldType.String => EncodeCompoundString(buffer, offset, (string)value),
                GaldrFieldType.Int32 => EncodeCompoundInt32(buffer, offset, (int)value),
                GaldrFieldType.Int64 => EncodeCompoundInt64(buffer, offset, (long)value),
                GaldrFieldType.Double => EncodeCompoundDouble(buffer, offset, (double)value),
                GaldrFieldType.Decimal => EncodeCompoundDecimal(buffer, offset, (decimal)value),
                GaldrFieldType.Boolean => EncodeCompoundBoolean(buffer, offset, (bool)value),
                GaldrFieldType.DateTime => EncodeCompoundDateTime(buffer, offset, (DateTime)value),
                GaldrFieldType.DateTimeOffset => EncodeCompoundDateTimeOffset(buffer, offset, (DateTimeOffset)value),
                GaldrFieldType.Guid => EncodeCompoundGuid(buffer, offset, (Guid)value),
                GaldrFieldType.Byte => EncodeCompoundByte(buffer, offset, (byte)value),
                GaldrFieldType.SByte => EncodeCompoundSByte(buffer, offset, (sbyte)value),
                GaldrFieldType.Int16 => EncodeCompoundInt16(buffer, offset, (short)value),
                GaldrFieldType.UInt16 => EncodeCompoundUInt16(buffer, offset, (ushort)value),
                GaldrFieldType.UInt32 => EncodeCompoundUInt32(buffer, offset, (uint)value),
                GaldrFieldType.UInt64 => EncodeCompoundUInt64(buffer, offset, (ulong)value),
                GaldrFieldType.Single => EncodeCompoundSingle(buffer, offset, (float)value),
                GaldrFieldType.Char => EncodeCompoundChar(buffer, offset, (char)value),
                GaldrFieldType.TimeSpan => EncodeCompoundTimeSpan(buffer, offset, (TimeSpan)value),
                GaldrFieldType.DateOnly => EncodeCompoundDateOnly(buffer, offset, (DateOnly)value),
                GaldrFieldType.TimeOnly => EncodeCompoundTimeOnly(buffer, offset, (TimeOnly)value),
                _ => throw new NotSupportedException($"Field type {fieldType} is not supported for compound encoding")
            };
        }
        return result;
    }
}
