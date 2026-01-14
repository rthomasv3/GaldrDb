using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

public interface IGaldrTypeInfo
{
    Type DocumentType { get; }
    string CollectionName { get; }
    IReadOnlyList<string> IndexedFieldNames { get; }
    IReadOnlyList<string> UniqueIndexFieldNames { get; }
    void ExtractIndexedFieldsFrom(object document, IndexFieldWriter writer);
    int GetIdFrom(object document);
}
