using System.Collections.Generic;

namespace GaldrDbEngine.Query.Execution;

internal sealed class QueryScanResult<TDocument>
{
    public List<TDocument> Documents { get; }
    public HashSet<int> DocumentIds { get; }

    public QueryScanResult(List<TDocument> documents, HashSet<int> documentIds)
    {
        Documents = documents;
        DocumentIds = documentIds;
    }
}
