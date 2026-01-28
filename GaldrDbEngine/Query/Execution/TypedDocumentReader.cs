using System.Collections.Generic;
using System.Text;
using GaldrJson;

namespace GaldrDbEngine.Query.Execution;

internal sealed class TypedDocumentReader<T> : IDocumentReader<T>
{
    private readonly GaldrTypeInfo<T> _typeInfo;
    private readonly IGaldrJsonSerializer _jsonSerializer;
    private readonly GaldrJsonOptions _jsonOptions;

    public TypedDocumentReader(
        GaldrTypeInfo<T> typeInfo,
        IGaldrJsonSerializer jsonSerializer,
        GaldrJsonOptions jsonOptions)
    {
        _typeInfo = typeInfo;
        _jsonSerializer = jsonSerializer;
        _jsonOptions = jsonOptions;
    }

    public T ReadDocument(byte[] jsonBytes)
    {
        string json = Encoding.UTF8.GetString(jsonBytes);
        return _jsonSerializer.Deserialize<T>(json, _jsonOptions);
    }

    public int GetDocumentId(T document)
    {
        return _typeInfo.IdGetter(document);
    }

    public bool PassesFilters(T document, IReadOnlyList<IFieldFilter> filters)
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
