using System.Collections.Generic;

namespace GaldrDbEngine.Storage;

internal class SecondaryIndexFlushedOps
{
    public SecondaryIndexBTree Tree { get; }
    public List<SecondaryFlushedOp> Ops { get; }

    public SecondaryIndexFlushedOps(SecondaryIndexBTree tree, List<SecondaryFlushedOp> ops)
    {
        Tree = tree;
        Ops = ops;
    }
}
