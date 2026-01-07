using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public interface IGaldrTypeInfo
{
    Type DocumentType { get; }
    string CollectionName { get; }
    IReadOnlyList<string> IndexedFieldNames { get; }
}
