using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class SecondaryIndexSpec
{
    public IndexDefinition IndexDefinition { get; }
    public IFieldFilter IndexFilter { get; }
    public SecondaryIndexOperation Operation { get; }

    public SecondaryIndexSpec(IndexDefinition indexDefinition, IFieldFilter indexFilter, SecondaryIndexOperation operation)
    {
        IndexDefinition = indexDefinition;
        IndexFilter = indexFilter;
        Operation = operation;
    }
}
