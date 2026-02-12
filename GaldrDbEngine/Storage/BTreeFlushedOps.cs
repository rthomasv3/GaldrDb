using System.Collections.Generic;

namespace GaldrDbEngine.Storage;

internal class BTreeFlushedOps
{
    public List<FlushedOp> PrimaryOps { get; }
    public List<SecondaryIndexFlushedOps> SecondaryOps { get; }

    public BTreeFlushedOps()
    {
        PrimaryOps = new List<FlushedOp>();
        SecondaryOps = new List<SecondaryIndexFlushedOps>();
    }
}
