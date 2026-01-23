using System;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace GaldrDbEngine.Query;

/// <summary>
/// Represents an ordering clause for dynamic query results.
/// </summary>
internal sealed class DynamicOrderByClause
{
    private readonly string _fieldName;
    private readonly bool _descending;

    public string FieldName
    {
        get { return _fieldName; }
    }

    public bool Descending
    {
        get { return _descending; }
    }

    public DynamicOrderByClause(string fieldName, bool descending)
    {
        _fieldName = fieldName;
        _descending = descending;
    }

    public int Compare(Json.JsonDocument a, Json.JsonDocument b)
    {
        object valA = GetFieldValue(a);
        object valB = GetFieldValue(b);

        int result;

        if (valA == null && valB == null)
        {
            result = 0;
        }
        else if (valA == null)
        {
            result = _descending ? 1 : -1;
        }
        else if (valB == null)
        {
            result = _descending ? -1 : 1;
        }
        else if (valA is IComparable comparable)
        {
            int cmp = comparable.CompareTo(valB);
            result = _descending ? -cmp : cmp;
        }
        else
        {
            result = 0;
        }

        return result;
    }

    private object GetFieldValue(Json.JsonDocument doc)
    {
        object result = null;

        if (doc.TryGetValue(_fieldName, out JsonNode node) && node != null)
        {
            result = ExtractValue(node);
        }

        return result;
    }

    private object ExtractValue(JsonNode node)
    {
        JsonValue jsonValue = node.AsValue();
        JsonElement element = jsonValue.GetValue<JsonElement>();

        object result = element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out long l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => node.ToJsonString()
        };

        return result;
    }
}
