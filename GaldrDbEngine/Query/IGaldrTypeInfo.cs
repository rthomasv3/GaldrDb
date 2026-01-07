using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public interface IGaldrTypeInfo
{
    string CollectionName { get; }
    IReadOnlyList<string> IndexedFieldNames { get; }
}
