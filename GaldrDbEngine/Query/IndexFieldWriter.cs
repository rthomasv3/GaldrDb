using System;
using System.Collections.Generic;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query;

/// <summary>
/// Writes indexed field values for secondary index updates. Used by source-generated code.
/// </summary>
public sealed class IndexFieldWriter
{
    private readonly List<IndexFieldEntry> _fields = new List<IndexFieldEntry>();

    /// <summary>Writes a string field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteString(string fieldName, string value)
    {
        if (value != null)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.String)));
        }
    }

    /// <summary>Writes an int field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt32(string fieldName, int value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int32)));
    }

    /// <summary>Writes a nullable int field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt32(string fieldName, int? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Int32)));
        }
    }

    /// <summary>Writes a long field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt64(string fieldName, long value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int64)));
    }

    /// <summary>Writes a nullable long field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt64(string fieldName, long? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Int64)));
        }
    }

    /// <summary>Writes a double field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDouble(string fieldName, double value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Double)));
    }

    /// <summary>Writes a nullable double field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDouble(string fieldName, double? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Double)));
        }
    }

    /// <summary>Writes a decimal field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDecimal(string fieldName, decimal value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Decimal)));
    }

    /// <summary>Writes a nullable decimal field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDecimal(string fieldName, decimal? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Decimal)));
        }
    }

    /// <summary>Writes a bool field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteBoolean(string fieldName, bool value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Boolean)));
    }

    /// <summary>Writes a nullable bool field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteBoolean(string fieldName, bool? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Boolean)));
        }
    }

    /// <summary>Writes a DateTime field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTime(string fieldName, DateTime value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTime)));
    }

    /// <summary>Writes a nullable DateTime field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTime(string fieldName, DateTime? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.DateTime)));
        }
    }

    /// <summary>Writes a DateTimeOffset field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTimeOffset(string fieldName, DateTimeOffset value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTimeOffset)));
    }

    /// <summary>Writes a nullable DateTimeOffset field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTimeOffset(string fieldName, DateTimeOffset? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.DateTimeOffset)));
        }
    }

    /// <summary>Writes a Guid field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteGuid(string fieldName, Guid value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Guid)));
    }

    /// <summary>Writes a nullable Guid field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteGuid(string fieldName, Guid? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Guid)));
        }
    }

    /// <summary>Writes a byte field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteByte(string fieldName, byte value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Byte)));
    }

    /// <summary>Writes a nullable byte field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteByte(string fieldName, byte? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Byte)));
        }
    }

    /// <summary>Writes an sbyte field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteSByte(string fieldName, sbyte value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.SByte)));
    }

    /// <summary>Writes a nullable sbyte field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteSByte(string fieldName, sbyte? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.SByte)));
        }
    }

    /// <summary>Writes a short field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt16(string fieldName, short value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int16)));
    }

    /// <summary>Writes a nullable short field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt16(string fieldName, short? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Int16)));
        }
    }

    /// <summary>Writes a ushort field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt16(string fieldName, ushort value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.UInt16)));
    }

    /// <summary>Writes a nullable ushort field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt16(string fieldName, ushort? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.UInt16)));
        }
    }

    /// <summary>Writes a uint field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt32(string fieldName, uint value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.UInt32)));
    }

    /// <summary>Writes a nullable uint field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt32(string fieldName, uint? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.UInt32)));
        }
    }

    /// <summary>Writes a ulong field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt64(string fieldName, ulong value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.UInt64)));
    }

    /// <summary>Writes a nullable ulong field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteUInt64(string fieldName, ulong? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.UInt64)));
        }
    }

    /// <summary>Writes a float field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteSingle(string fieldName, float value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Single)));
    }

    /// <summary>Writes a nullable float field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteSingle(string fieldName, float? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Single)));
        }
    }

    /// <summary>Writes a char field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteChar(string fieldName, char value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Char)));
    }

    /// <summary>Writes a nullable char field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteChar(string fieldName, char? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.Char)));
        }
    }

    /// <summary>Writes a TimeSpan field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteTimeSpan(string fieldName, TimeSpan value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.TimeSpan)));
    }

    /// <summary>Writes a nullable TimeSpan field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteTimeSpan(string fieldName, TimeSpan? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.TimeSpan)));
        }
    }

    /// <summary>Writes a DateOnly field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateOnly(string fieldName, DateOnly value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateOnly)));
    }

    /// <summary>Writes a nullable DateOnly field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateOnly(string fieldName, DateOnly? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.DateOnly)));
        }
    }

    /// <summary>Writes a TimeOnly field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteTimeOnly(string fieldName, TimeOnly value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.TimeOnly)));
    }

    /// <summary>Writes a nullable TimeOnly field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteTimeOnly(string fieldName, TimeOnly? value)
    {
        if (value.HasValue)
        {
            _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value.Value, GaldrFieldType.TimeOnly)));
        }
    }

    /// <summary>Gets all written field entries.</summary>
    /// <returns>The field entries.</returns>
    public IReadOnlyList<IndexFieldEntry> GetFields()
    {
        return _fields;
    }

    /// <summary>Clears all written field entries.</summary>
    public void Clear()
    {
        _fields.Clear();
    }
}
