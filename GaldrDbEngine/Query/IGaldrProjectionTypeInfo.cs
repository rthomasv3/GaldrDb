using System;
using GaldrJson;

namespace GaldrDbEngine.Query;

public interface IGaldrProjectionTypeInfo : IGaldrTypeInfo
{
    Type SourceType { get; }

    object DeserializeSource(string json, IGaldrJsonSerializer serializer, GaldrJsonOptions options);

    object ConvertToProjection(object source);

    int GetSourceId(object source);
}
