using System.Collections.Generic;

namespace GaldrDbEngine.Schema;

public class OrphanedSchemaInfo
{
    public IReadOnlyList<string> Collections { get; }
    public IReadOnlyList<OrphanedIndexInfo> Indexes { get; }

    public OrphanedSchemaInfo(IReadOnlyList<string> collections, IReadOnlyList<OrphanedIndexInfo> indexes)
    {
        Collections = collections;
        Indexes = indexes;
    }

    public bool HasOrphans
    {
        get { return Collections.Count > 0 || Indexes.Count > 0; }
    }
}
