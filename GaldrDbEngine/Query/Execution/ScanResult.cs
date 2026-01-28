using System.Collections.Generic;
using GaldrDbEngine.MVCC;

namespace GaldrDbEngine.Query.Execution;

internal sealed class ScanResult
{
    public List<DocumentVersion> Versions { get; }
    public HashSet<int> ScannedDocIds { get; }

    public ScanResult(List<DocumentVersion> versions, HashSet<int> scannedDocIds)
    {
        Versions = versions;
        ScannedDocIds = scannedDocIds;
    }

    public static ScanResult Empty()
    {
        return new ScanResult(new List<DocumentVersion>(), new HashSet<int>());
    }
}
