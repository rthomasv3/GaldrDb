using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class IndexFieldWriter
{
    private readonly List<(string FieldName, byte[] KeyBytes)> _fields = new List<(string, byte[])>();

    public void WriteString(string fieldName, string value)
    {
        if (value != null)
        {
            _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.String)));
        }
    }

    public void WriteInt32(string fieldName, int value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int32)));
    }

    public void WriteInt64(string fieldName, long value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Int64)));
    }

    public void WriteDouble(string fieldName, double value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Double)));
    }

    public void WriteDecimal(string fieldName, decimal value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Decimal)));
    }

    public void WriteBoolean(string fieldName, bool value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Boolean)));
    }

    public void WriteDateTime(string fieldName, DateTime value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTime)));
    }

    public void WriteDateTimeOffset(string fieldName, DateTimeOffset value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.DateTimeOffset)));
    }

    public void WriteGuid(string fieldName, Guid value)
    {
        _fields.Add((fieldName, IndexKeyEncoder.Encode(value, GaldrFieldType.Guid)));
    }

    public IReadOnlyList<(string FieldName, byte[] KeyBytes)> GetFields()
    {
        return _fields;
    }

    public void Clear()
    {
        _fields.Clear();
    }
}
