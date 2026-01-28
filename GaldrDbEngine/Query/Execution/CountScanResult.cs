using System.Collections.Generic;

namespace GaldrDbEngine.Query.Execution;

internal sealed class CountScanResult
{
    public int Count { get; }
    public HashSet<int> DocumentIds { get; }

    public CountScanResult(int count, HashSet<int> documentIds)
    {
        Count = count;
        DocumentIds = documentIds;
    }
}
