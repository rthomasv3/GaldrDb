using System.Collections.Generic;
using System.Text;
using GaldrJson;

namespace GaldrDbEngine.Query.Execution;

internal sealed class ProjectionDocumentReader : IDocumentReader<object>
{
    private readonly IGaldrProjectionTypeInfo _projTypeInfo;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;

    public ProjectionDocumentReader(
        IGaldrProjectionTypeInfo projTypeInfo,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _projTypeInfo = projTypeInfo;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
    }

    public object ReadDocument(byte[] jsonBytes)
    {
        string json = Encoding.UTF8.GetString(jsonBytes);
        return _projTypeInfo.DeserializeSource(json, _jsonSerializer, _jsonOptions);
    }

    public int GetDocumentId(object document)
    {
        return _projTypeInfo.GetSourceId(document);
    }

    public bool PassesFilters(object document, IReadOnlyList<IFieldFilter> filters)
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
