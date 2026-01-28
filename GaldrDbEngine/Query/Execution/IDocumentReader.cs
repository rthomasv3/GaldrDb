using System.Collections.Generic;

namespace GaldrDbEngine.Query.Execution;

internal interface IDocumentReader<TDocument>
{
    TDocument ReadDocument(byte[] jsonBytes);
    int GetDocumentId(TDocument document);
    bool PassesFilters(TDocument document, IReadOnlyList<IFieldFilter> filters);
}
