using System;
using System.Collections.Generic;
using System.Text;
using GaldrDbEngine.Query;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class IndexDefinition
{
    private List<IndexField> _fields;
    private string _indexName;

    public IReadOnlyList<IndexField> Fields => _fields;

    public string IndexName
    {
        get
        {
            if (_indexName == null)
            {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < _fields.Count; i++)
                {
                    if (i > 0)
                    {
                        sb.Append('_');
                    }
                    sb.Append(_fields[i].FieldName);
                }
                _indexName = sb.ToString();
            }
            return _indexName;
        }
    }

    public bool IsCompound => _fields.Count > 1;

    public string FieldName => _fields.Count > 0 ? _fields[0].FieldName : null;

    public GaldrFieldType FieldType => _fields.Count > 0 ? _fields[0].FieldType : GaldrFieldType.String;

    public int RootPageId { get; set; }

    public bool IsUnique { get; set; }

    public IndexDefinition()
    {
        _fields = new List<IndexField>();
    }

    public IndexDefinition(string fieldName, GaldrFieldType fieldType, int rootPageId, bool isUnique)
    {
        _fields = new List<IndexField> { new IndexField(fieldName, fieldType) };
        RootPageId = rootPageId;
        IsUnique = isUnique;
    }

    public IndexDefinition(IReadOnlyList<IndexField> fields, int rootPageId, bool isUnique)
    {
        _fields = new List<IndexField>(fields);
        RootPageId = rootPageId;
        IsUnique = isUnique;
    }

    public void AddField(string fieldName, GaldrFieldType fieldType)
    {
        _fields.Add(new IndexField(fieldName, fieldType));
        _indexName = null;
    }

    public int SerializeTo(byte[] buffer, int startOffset)
    {
        int offset = startOffset;

        buffer[offset] = (byte)_fields.Count;
        offset += 1;

        for (int i = 0; i < _fields.Count; i++)
        {
            IndexField field = _fields[i];
            int nameByteCount = Encoding.UTF8.GetByteCount(field.FieldName);

            BinaryHelper.WriteUInt16BE(buffer, offset, (ushort)nameByteCount);
            offset += 2;

            Encoding.UTF8.GetBytes(field.FieldName, 0, field.FieldName.Length, buffer, offset);
            offset += nameByteCount;

            buffer[offset] = (byte)field.FieldType;
            offset += 1;
        }

        BinaryHelper.WriteInt32LE(buffer, offset, RootPageId);
        offset += 4;

        buffer[offset] = IsUnique ? (byte)1 : (byte)0;
        offset += 1;

        return offset - startOffset;
    }

    public static IndexDefinition Deserialize(byte[] buffer, int startOffset, out int bytesRead)
    {
        IndexDefinition index = new IndexDefinition();
        int offset = startOffset;

        bool isOldFormat = buffer[offset + 1] == 0 && buffer[offset + 2] == 0 && buffer[offset + 3] == 0;

        if (isOldFormat)
        {
            int nameLength = BinaryHelper.ReadInt32LE(buffer, offset);
            offset += 4;

            byte[] nameBytes = new byte[nameLength];
            Array.Copy(buffer, offset, nameBytes, 0, nameLength);
            string fieldName = Encoding.UTF8.GetString(nameBytes);
            offset += nameLength;

            GaldrFieldType fieldType = (GaldrFieldType)buffer[offset];
            offset += 1;

            index._fields.Add(new IndexField(fieldName, fieldType));

            index.RootPageId = BinaryHelper.ReadInt32LE(buffer, offset);
            offset += 4;

            index.IsUnique = buffer[offset] == 1;
            offset += 1;
        }
        else
        {
            int fieldCount = buffer[offset];
            offset += 1;

            for (int i = 0; i < fieldCount; i++)
            {
                int nameLength = BinaryHelper.ReadUInt16BE(buffer, offset);
                offset += 2;

                byte[] nameBytes = new byte[nameLength];
                Array.Copy(buffer, offset, nameBytes, 0, nameLength);
                string fieldName = Encoding.UTF8.GetString(nameBytes);
                offset += nameLength;

                GaldrFieldType fieldType = (GaldrFieldType)buffer[offset];
                offset += 1;

                index._fields.Add(new IndexField(fieldName, fieldType));
            }

            index.RootPageId = BinaryHelper.ReadInt32LE(buffer, offset);
            offset += 4;

            index.IsUnique = buffer[offset] == 1;
            offset += 1;
        }

        bytesRead = offset - startOffset;

        return index;
    }

    public int GetSerializedSize()
    {
        int size = 1;

        for (int i = 0; i < _fields.Count; i++)
        {
            int nameByteCount = Encoding.UTF8.GetByteCount(_fields[i].FieldName);
            size += 2 + nameByteCount + 1;
        }

        size += 4 + 1;

        return size;
    }
}
