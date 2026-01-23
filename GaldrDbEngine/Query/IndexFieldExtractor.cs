using System;
using System.Collections.Generic;
using System.Text.Json;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query;

/// <summary>
/// Extracts index field values from JSON bytes using index definitions.
/// Uses Utf8JsonReader for minimal allocations.
/// </summary>
internal static class IndexFieldExtractor
{
    /// <summary>
    /// Extracts index field entries from raw JSON bytes based on the collection's index definitions.
    /// Uses forward-only Utf8JsonReader to avoid DOM allocations.
    /// </summary>
    /// <param name="jsonBytes">The UTF-8 encoded JSON bytes.</param>
    /// <param name="indexes">The index definitions specifying which fields to extract and their types.</param>
    /// <returns>A list of index field entries with encoded key bytes.</returns>
    public static IReadOnlyList<IndexFieldEntry> ExtractFromBytes(
        byte[] jsonBytes,
        IReadOnlyList<IndexDefinition> indexes)
    {
        List<IndexFieldEntry> fields = new List<IndexFieldEntry>(indexes.Count);
        Utf8JsonReader reader = new Utf8JsonReader(jsonBytes);

        // Move to start of object
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            return fields;
        }

        int foundCount = 0;
        int indexCount = indexes.Count;

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            // Check if this property is one of our indexed fields
            IndexDefinition matchedIndex = null;
            for (int i = 0; i < indexCount; i++)
            {
                if (reader.ValueTextEquals(indexes[i].FieldName))
                {
                    matchedIndex = indexes[i];
                    break;
                }
            }

            // Move to the value
            reader.Read();

            if (matchedIndex != null)
            {
                byte[] keyBytes;
                if (reader.TokenType == JsonTokenType.Null)
                {
                    keyBytes = IndexKeyEncoder.Encode(null, matchedIndex.FieldType);
                }
                else
                {
                    keyBytes = ExtractAndEncode(ref reader, matchedIndex.FieldType);
                }

                if (keyBytes != null)
                {
                    fields.Add(new IndexFieldEntry(matchedIndex.FieldName, keyBytes));
                    foundCount++;

                    // Early exit if we found all indexed fields
                    if (foundCount == indexCount)
                    {
                        break;
                    }
                }
            }
            else
            {
                // Skip the value we don't need
                reader.Skip();
            }
        }

        return fields;
    }

    /// <summary>
    /// Extracts a value from the reader and encodes it as index key bytes.
    /// </summary>
    private static byte[] ExtractAndEncode(ref Utf8JsonReader reader, GaldrFieldType fieldType)
    {
        return fieldType switch
        {
            GaldrFieldType.String => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(reader.GetString(), fieldType)
                : null,
            GaldrFieldType.Int32 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetInt32(), fieldType)
                : null,
            GaldrFieldType.Int64 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetInt64(), fieldType)
                : null,
            GaldrFieldType.Double => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetDouble(), fieldType)
                : null,
            GaldrFieldType.Decimal => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetDecimal(), fieldType)
                : null,
            GaldrFieldType.Boolean => reader.TokenType == JsonTokenType.True || reader.TokenType == JsonTokenType.False
                ? IndexKeyEncoder.Encode(reader.GetBoolean(), fieldType)
                : null,
            GaldrFieldType.DateTime => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(reader.GetDateTime(), fieldType)
                : null,
            GaldrFieldType.DateTimeOffset => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(reader.GetDateTimeOffset(), fieldType)
                : null,
            GaldrFieldType.Guid => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(reader.GetGuid(), fieldType)
                : null,
            GaldrFieldType.Byte => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetByte(), fieldType)
                : null,
            GaldrFieldType.SByte => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetSByte(), fieldType)
                : null,
            GaldrFieldType.Int16 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetInt16(), fieldType)
                : null,
            GaldrFieldType.UInt16 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetUInt16(), fieldType)
                : null,
            GaldrFieldType.UInt32 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetUInt32(), fieldType)
                : null,
            GaldrFieldType.UInt64 => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetUInt64(), fieldType)
                : null,
            GaldrFieldType.Single => reader.TokenType == JsonTokenType.Number
                ? IndexKeyEncoder.Encode(reader.GetSingle(), fieldType)
                : null,
            GaldrFieldType.Char => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(reader.GetString()[0], fieldType)
                : null,
            GaldrFieldType.TimeSpan => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(TimeSpan.Parse(reader.GetString()), fieldType)
                : null,
            GaldrFieldType.DateOnly => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(DateOnly.Parse(reader.GetString()), fieldType)
                : null,
            GaldrFieldType.TimeOnly => reader.TokenType == JsonTokenType.String
                ? IndexKeyEncoder.Encode(TimeOnly.Parse(reader.GetString()), fieldType)
                : null,
            _ => null
        };
    }
}
