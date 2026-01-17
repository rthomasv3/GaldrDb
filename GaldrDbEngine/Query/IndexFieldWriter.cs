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

    /// <summary>Writes a long field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteInt64(string fieldName, long value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int64)));
    }

    /// <summary>Writes a double field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDouble(string fieldName, double value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Double)));
    }

    /// <summary>Writes a decimal field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDecimal(string fieldName, decimal value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Decimal)));
    }

    /// <summary>Writes a bool field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteBoolean(string fieldName, bool value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Boolean)));
    }

    /// <summary>Writes a DateTime field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTime(string fieldName, DateTime value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTime)));
    }

    /// <summary>Writes a DateTimeOffset field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteDateTimeOffset(string fieldName, DateTimeOffset value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTimeOffset)));
    }

    /// <summary>Writes a Guid field value.</summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="value">The value.</param>
    public void WriteGuid(string fieldName, Guid value)
    {
        _fields.Add(new IndexFieldEntry(fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Guid)));
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
