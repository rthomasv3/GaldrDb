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

            Dictionary<string, ExtractedValue> extractedValues = new Dictionary<string, ExtractedValue>();
            Utf8JsonReader reader = new Utf8JsonReader(jsonBytes);

            if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName && extractedValues.Count < neededFields.Count)
                {
                    string propertyName = reader.GetString();
                    reader.Read();

                    if (neededFields.Contains(propertyName))
                    {
                        GaldrFieldType fieldType = GetFieldTypeFor(propertyName, indexes);
                        ExtractedValue extracted = ExtractValue(ref reader, fieldType);
                        extractedValues[propertyName] = extracted;
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
                if (extractedValues.TryGetValue(index.FieldName, out ExtractedValue value))
                {
                    keyBytes = IndexKeyEncoder.Encode(value.Value, index.FieldType);
                }
                else
                {
                    keyBytes = IndexKeyEncoder.Encode(null, index.FieldType);
                }
                fields.Add(new IndexFieldEntry(index.FieldName, keyBytes));
            }

            foreach (IndexDefinition index in compoundIndexes)
            {
                object[] values = new object[index.Fields.Count];
                GaldrFieldType[] fieldTypes = new GaldrFieldType[index.Fields.Count];

                for (int i = 0; i < index.Fields.Count; i++)
                {
                    IndexField indexField = index.Fields[i];
                    fieldTypes[i] = indexField.FieldType;

                    if (extractedValues.TryGetValue(indexField.FieldName, out ExtractedValue value))
                    {
                        values[i] = value.Value;
                    }
                    else
                    {
                        values[i] = null;
                    }
                }

                byte[] keyBytes = IndexKeyEncoder.EncodeCompound(values, fieldTypes);
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

    private static ExtractedValue ExtractValue(ref Utf8JsonReader reader, GaldrFieldType fieldType)
    {
        object value = null;

        if (reader.TokenType != JsonTokenType.Null)
        {
            value = fieldType switch
            {
                GaldrFieldType.String => reader.TokenType == JsonTokenType.String
                    ? reader.GetString()
                    : null,
                GaldrFieldType.Int32 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetInt32()
                    : null,
                GaldrFieldType.Int64 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetInt64()
                    : null,
                GaldrFieldType.Double => reader.TokenType == JsonTokenType.Number
                    ? reader.GetDouble()
                    : null,
                GaldrFieldType.Decimal => reader.TokenType == JsonTokenType.Number
                    ? reader.GetDecimal()
                    : null,
                GaldrFieldType.Boolean => reader.TokenType == JsonTokenType.True || reader.TokenType == JsonTokenType.False
                    ? reader.GetBoolean()
                    : null,
                GaldrFieldType.DateTime => reader.TokenType == JsonTokenType.String
                    ? reader.GetDateTime()
                    : null,
                GaldrFieldType.DateTimeOffset => reader.TokenType == JsonTokenType.String
                    ? reader.GetDateTimeOffset()
                    : null,
                GaldrFieldType.Guid => reader.TokenType == JsonTokenType.String
                    ? reader.GetGuid()
                    : null,
                GaldrFieldType.Byte => reader.TokenType == JsonTokenType.Number
                    ? reader.GetByte()
                    : null,
                GaldrFieldType.SByte => reader.TokenType == JsonTokenType.Number
                    ? reader.GetSByte()
                    : null,
                GaldrFieldType.Int16 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetInt16()
                    : null,
                GaldrFieldType.UInt16 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetUInt16()
                    : null,
                GaldrFieldType.UInt32 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetUInt32()
                    : null,
                GaldrFieldType.UInt64 => reader.TokenType == JsonTokenType.Number
                    ? reader.GetUInt64()
                    : null,
                GaldrFieldType.Single => reader.TokenType == JsonTokenType.Number
                    ? reader.GetSingle()
                    : null,
                GaldrFieldType.Char => reader.TokenType == JsonTokenType.String && reader.GetString().Length > 0
                    ? reader.GetString()[0]
                    : null,
                GaldrFieldType.TimeSpan => reader.TokenType == JsonTokenType.String
                    ? TimeSpan.Parse(reader.GetString())
                    : null,
                GaldrFieldType.DateOnly => reader.TokenType == JsonTokenType.String
                    ? DateOnly.Parse(reader.GetString())
                    : null,
                GaldrFieldType.TimeOnly => reader.TokenType == JsonTokenType.String
                    ? TimeOnly.Parse(reader.GetString())
                    : null,
                _ => null
            };
        }

        return new ExtractedValue(value);
    }
}
