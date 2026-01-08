using System.Collections.Generic;

namespace GaldrDbEngine.MVCC;

public sealed class CollectionVersions
{
    public string CollectionName { get; }
    public List<DocumentVersionChain> Chains { get; }

    public CollectionVersions(string collectionName, List<DocumentVersionChain> chains)
    {
        CollectionName = collectionName;
        Chains = chains;
    }
}
