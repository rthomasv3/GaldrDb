using System.Collections.Generic;
using System.Text;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query.Execution;

internal sealed class DynamicDocumentReader : IDocumentReader<JsonDocument>
{
    public JsonDocument ReadDocument(byte[] jsonBytes)
    {
        string json = Encoding.UTF8.GetString(jsonBytes);
        return JsonDocument.Parse(json);
    }

    public int GetDocumentId(JsonDocument document)
    {
        return document.GetInt32("Id");
    }

    public bool PassesFilters(JsonDocument document, IReadOnlyList<IFieldFilter> filters)
    {
        bool passes = true;

        foreach (IFieldFilter filter in filters)
        {
            if (!filter.Evaluate(document))
            {
                passes = false;
                break;
            }
        }

        return passes;
    }
}
