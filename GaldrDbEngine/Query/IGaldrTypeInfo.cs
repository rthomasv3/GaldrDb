using System.Collections.Generic;

namespace GaldrDbCore.Query;

public interface IGaldrTypeInfo
{
    string CollectionName { get; }
    IReadOnlyList<string> IndexedFieldNames { get; }
}
