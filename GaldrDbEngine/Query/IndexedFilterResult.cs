using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

internal sealed class IndexedFilterResult
{
    public IFieldFilter Filter { get; }
    public IndexDefinition IndexDefinition { get; }
    public int FilterIndex { get; }

    public IndexedFilterResult(IFieldFilter filter, IndexDefinition indexDefinition, int filterIndex)
    {
        Filter = filter;
        IndexDefinition = indexDefinition;
        FilterIndex = filterIndex;
    }
}
