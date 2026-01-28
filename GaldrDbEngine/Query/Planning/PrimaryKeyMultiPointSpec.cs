using System.Collections.Generic;

namespace GaldrDbEngine.Query.Planning;

internal sealed class PrimaryKeyMultiPointSpec
{
    public IReadOnlyList<int> DocIds { get; }

    public PrimaryKeyMultiPointSpec(IReadOnlyList<int> docIds)
    {
        DocIds = docIds;
    }
}
