using System;
using System.Collections.Generic;
using System.Text.Json;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query;

/// <summary>
/// Extracts index field values from JSON bytes using index definitions.
/// Encodes values directly during extraction to avoid boxing.
/// </summary>
internal static class IndexFieldExtractor
{
    private static readonly byte[] _nullEncodedBytes = new byte[] { 0x00 };

    /// <summary>
    /// Extracts index field entries from raw JSON bytes based on the collection's index definitions.
    /// Encodes values directly during JSON parsing to avoid boxing value types.
    /// </summary>
    /// <param name="jsonBytes">The UTF-8 encoded JSON bytes.</param>
    /// <param name="indexes">The index definitions specifying which fields to extract and their types.</param>
    /// <returns>A list of index field entries with encoded key bytes.</returns>
    public static IReadOnlyList<IndexFieldEntry> ExtractFromBytes(
        byte[] jsonBytes,
        IReadOnlyList<IndexDefinition> indexes)
    {
        List<IndexFieldEntry> fields = new List<IndexFieldEntry>();

        if (indexes.Count > 0)
        {
            List<IndexDefinition> singleFieldIndexes = new List<IndexDefinition>();
            List<IndexDefinition> compoundIndexes = new List<IndexDefinition>();
            HashSet<string> neededFields = new HashSet<string>();

            foreach (IndexDefinition index in indexes)
            {
                if (index.IsCompound)
                {
                    compoundIndexes.Add(index);
                    foreach (IndexField field in index.Fields)
                    {
                        neededFields.Add(field.FieldName);
                    }
                }
                else
                {
                    singleFieldIndexes.Add(index);
                    neededFields.Add(index.FieldName);
                }
            }

            Dictionary<string, byte[]> encodedFields = new Dictionary<string, byte[]>();
            Utf8JsonReader reader = new Utf8JsonReader(jsonBytes);

            if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && encodedFields.Count < neededFields.Count)
                {
                    string propertyName = reader.GetString();
                    reader.Read();

                    if (neededFields.Contains(propertyName))
                    {
                        GaldrFieldType fieldType = GetFieldTypeFor(propertyName, indexes);
                        byte[] encoded = EncodeValueDirect(ref reader, fieldType);
                        encodedFields[propertyName] = encoded;
                    }
                    else
                    {
                        reader.Skip();
                    }
                }
            }

            foreach (IndexDefinition index in singleFieldIndexes)
            {
                byte[] keyBytes;
                if (encodedFields.TryGetValue(index.FieldName, out byte[] encoded))
                {
                    keyBytes = encoded;
                }
                else
                {
                    keyBytes = _nullEncodedBytes;
                }
                fields.Add(new IndexFieldEntry(index.FieldName, keyBytes));
            }

            foreach (IndexDefinition index in compoundIndexes)
            {
                byte[] keyBytes = ConcatenateEncodedFields(index, encodedFields);
                fields.Add(new IndexFieldEntry(index.IndexName, keyBytes));
            }
        }

        return fields;
    }

    private static GaldrFieldType GetFieldTypeFor(string fieldName, IReadOnlyList<IndexDefinition> indexes)
    {
        GaldrFieldType result = GaldrFieldType.String;
        bool found = false;

        for (int i = 0; i < indexes.Count && !found; i++)
        {
            IndexDefinition index = indexes[i];
            if (index.IsCompound)
            {
                foreach (IndexField field in index.Fields)
                {
                    if (field.FieldName == fieldName)
                    {
                        result = field.FieldType;
                        found = true;
                        break;
                    }
                }
            }
            else if (index.FieldName == fieldName)
            {
                result = index.FieldType;
                found = true;
            }
        }

        return result;
    }

    private static byte[] EncodeValueDirect(ref Utf8JsonReader reader, GaldrFieldType fieldType)
    {
        byte[] result;

        if (reader.TokenType == JsonTokenType.Null)
        {
            result = _nullEncodedBytes;
        }
        else
        {
            result = fieldType switch
            {
                GaldrFieldType.String => EncodeStringDirect(ref reader),
                GaldrFieldType.Int32 => EncodeInt32Direct(ref reader),
                GaldrFieldType.Int64 => EncodeInt64Direct(ref reader),
                GaldrFieldType.Double => EncodeDoubleDirect(ref reader),
                GaldrFieldType.Decimal => EncodeDecimalDirect(ref reader),
                GaldrFieldType.Boolean => EncodeBooleanDirect(ref reader),
                GaldrFieldType.DateTime => EncodeDateTimeDirect(ref reader),
                GaldrFieldType.DateTimeOffset => EncodeDateTimeOffsetDirect(ref reader),
                GaldrFieldType.Guid => EncodeGuidDirect(ref reader),
                GaldrFieldType.Byte => EncodeByteDirect(ref reader),
                GaldrFieldType.SByte => EncodeSByteDirect(ref reader),
                GaldrFieldType.Int16 => EncodeInt16Direct(ref reader),
                GaldrFieldType.UInt16 => EncodeUInt16Direct(ref reader),
                GaldrFieldType.UInt32 => EncodeUInt32Direct(ref reader),
                GaldrFieldType.UInt64 => EncodeUInt64Direct(ref reader),
                GaldrFieldType.Single => EncodeSingleDirect(ref reader),
                GaldrFieldType.Char => EncodeCharDirect(ref reader),
                GaldrFieldType.TimeSpan => EncodeTimeSpanDirect(ref reader),
                GaldrFieldType.DateOnly => EncodeDateOnlyDirect(ref reader),
                GaldrFieldType.TimeOnly => EncodeTimeOnlyDirect(ref reader),
                _ => _nullEncodedBytes
            };
        }

        return result;
    }

    private static byte[] EncodeStringDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            string value = reader.GetString();
            result = IndexKeyEncoder.Encode(value, GaldrFieldType.String);
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeInt32Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            int value = reader.GetInt32();
            byte[] encoded = new byte[5];
            encoded[0] = 0x01;
            uint sortable = (uint)(value ^ int.MinValue);
            encoded[1] = (byte)(sortable >> 24);
            encoded[2] = (byte)(sortable >> 16);
            encoded[3] = (byte)(sortable >> 8);
            encoded[4] = (byte)sortable;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeInt64Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            long value = reader.GetInt64();
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            ulong sortable = (ulong)(value ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(sortable >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeDoubleDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            double value = reader.GetDouble();
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            long bits = BitConverter.DoubleToInt64Bits(value);
            ulong sortable;
            if (bits < 0)
            {
                sortable = ((ulong)bits) ^ 0xFFFFFFFFFFFFFFFFUL;
            }
            else
            {
                sortable = (ulong)bits ^ 0x8000000000000000UL;
            }
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(sortable >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeDecimalDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            decimal value = reader.GetDecimal();
            int[] parts = decimal.GetBits(value);
            byte[] encoded = new byte[17];
            encoded[0] = 0x01;

            int sign = (parts[3] >> 31) & 1;
            int scale = (parts[3] >> 16) & 0x7F;

            encoded[1] = (byte)(sign == 0 ? 0x80 : 0x00);
            encoded[2] = (byte)scale;

            uint lo = (uint)parts[0];
            uint mid = (uint)parts[1];
            uint hi = (uint)parts[2];

            if (sign == 0)
            {
                encoded[3] = (byte)(hi >> 24);
                encoded[4] = (byte)(hi >> 16);
                encoded[5] = (byte)(hi >> 8);
                encoded[6] = (byte)hi;
                encoded[7] = (byte)(mid >> 24);
                encoded[8] = (byte)(mid >> 16);
                encoded[9] = (byte)(mid >> 8);
                encoded[10] = (byte)mid;
                encoded[11] = (byte)(lo >> 24);
                encoded[12] = (byte)(lo >> 16);
                encoded[13] = (byte)(lo >> 8);
                encoded[14] = (byte)lo;
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

                encoded[3] = (byte)(compHi >> 24);
                encoded[4] = (byte)(compHi >> 16);
                encoded[5] = (byte)(compHi >> 8);
                encoded[6] = (byte)compHi;
                encoded[7] = (byte)(complement >> 56);
                encoded[8] = (byte)(complement >> 48);
                encoded[9] = (byte)(complement >> 40);
                encoded[10] = (byte)(complement >> 32);
                encoded[11] = (byte)(complement >> 24);
                encoded[12] = (byte)(complement >> 16);
                encoded[13] = (byte)(complement >> 8);
                encoded[14] = (byte)complement;
            }

            encoded[15] = 0;
            encoded[16] = 0;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeBooleanDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.True || reader.TokenType == JsonTokenType.False)
        {
            bool value = reader.GetBoolean();
            byte[] encoded = new byte[2];
            encoded[0] = 0x01;
            encoded[1] = value ? (byte)1 : (byte)0;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeDateTimeDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            DateTime value = reader.GetDateTime();
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            ulong sortable = (ulong)(value.Ticks ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(sortable >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeDateTimeOffsetDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            DateTimeOffset value = reader.GetDateTimeOffset();
            byte[] encoded = new byte[17];
            encoded[0] = 0x01;
            ulong utcEncoded = (ulong)(value.UtcTicks ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(utcEncoded >> (56 - i * 8));
            }
            ulong offsetEncoded = (ulong)(value.Offset.Ticks ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[9 + i] = (byte)(offsetEncoded >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeGuidDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            Guid value = reader.GetGuid();
            byte[] encoded = new byte[17];
            encoded[0] = 0x01;
            byte[] guidBytes = value.ToByteArray();
            Array.Copy(guidBytes, 0, encoded, 1, 16);
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeByteDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            byte value = reader.GetByte();
            byte[] encoded = new byte[2];
            encoded[0] = 0x01;
            encoded[1] = value;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeSByteDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            sbyte value = reader.GetSByte();
            byte[] encoded = new byte[2];
            encoded[0] = 0x01;
            encoded[1] = (byte)(value ^ sbyte.MinValue);
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeInt16Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            short value = reader.GetInt16();
            byte[] encoded = new byte[3];
            encoded[0] = 0x01;
            ushort sortable = (ushort)(value ^ short.MinValue);
            encoded[1] = (byte)(sortable >> 8);
            encoded[2] = (byte)sortable;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeUInt16Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            ushort value = reader.GetUInt16();
            byte[] encoded = new byte[3];
            encoded[0] = 0x01;
            encoded[1] = (byte)(value >> 8);
            encoded[2] = (byte)value;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeUInt32Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            uint value = reader.GetUInt32();
            byte[] encoded = new byte[5];
            encoded[0] = 0x01;
            encoded[1] = (byte)(value >> 24);
            encoded[2] = (byte)(value >> 16);
            encoded[3] = (byte)(value >> 8);
            encoded[4] = (byte)value;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeUInt64Direct(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            ulong value = reader.GetUInt64();
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(value >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeSingleDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.Number)
        {
            float value = reader.GetSingle();
            byte[] encoded = new byte[5];
            encoded[0] = 0x01;
            int bits = BitConverter.SingleToInt32Bits(value);
            uint sortable;
            if (bits < 0)
            {
                sortable = ((uint)bits) ^ 0xFFFFFFFFU;
            }
            else
            {
                sortable = (uint)bits ^ 0x80000000U;
            }
            encoded[1] = (byte)(sortable >> 24);
            encoded[2] = (byte)(sortable >> 16);
            encoded[3] = (byte)(sortable >> 8);
            encoded[4] = (byte)sortable;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeCharDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            string str = reader.GetString();
            if (str.Length > 0)
            {
                char value = str[0];
                byte[] encoded = new byte[3];
                encoded[0] = 0x01;
                ushort charValue = value;
                encoded[1] = (byte)(charValue >> 8);
                encoded[2] = (byte)charValue;
                result = encoded;
            }
            else
            {
                result = _nullEncodedBytes;
            }
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeTimeSpanDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            TimeSpan value = TimeSpan.Parse(reader.GetString());
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            ulong sortable = (ulong)(value.Ticks ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(sortable >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeDateOnlyDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            DateOnly value = DateOnly.Parse(reader.GetString());
            byte[] encoded = new byte[5];
            encoded[0] = 0x01;
            uint sortable = (uint)(value.DayNumber ^ int.MinValue);
            encoded[1] = (byte)(sortable >> 24);
            encoded[2] = (byte)(sortable >> 16);
            encoded[3] = (byte)(sortable >> 8);
            encoded[4] = (byte)sortable;
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] EncodeTimeOnlyDirect(ref Utf8JsonReader reader)
    {
        byte[] result;
        if (reader.TokenType == JsonTokenType.String)
        {
            TimeOnly value = TimeOnly.Parse(reader.GetString());
            byte[] encoded = new byte[9];
            encoded[0] = 0x01;
            ulong sortable = (ulong)(value.Ticks ^ long.MinValue);
            for (int i = 0; i < 8; i++)
            {
                encoded[1 + i] = (byte)(sortable >> (56 - i * 8));
            }
            result = encoded;
        }
        else
        {
            result = _nullEncodedBytes;
        }
        return result;
    }

    private static byte[] ConcatenateEncodedFields(IndexDefinition index, Dictionary<string, byte[]> encodedFields)
    {
        int totalSize = 0;
        foreach (IndexField field in index.Fields)
        {
            if (encodedFields.TryGetValue(field.FieldName, out byte[] encoded))
            {
                totalSize += encoded.Length;
            }
            else
            {
                totalSize += 1;
            }
        }

        byte[] result = new byte[totalSize];
        int offset = 0;

        foreach (IndexField field in index.Fields)
        {
            byte[] encoded;
            if (encodedFields.TryGetValue(field.FieldName, out byte[] fieldEncoded))
            {
                encoded = fieldEncoded;
            }
            else
            {
                encoded = _nullEncodedBytes;
            }
            Buffer.BlockCopy(encoded, 0, result, offset, encoded.Length);
            offset += encoded.Length;
        }

        return result;
    }
}
