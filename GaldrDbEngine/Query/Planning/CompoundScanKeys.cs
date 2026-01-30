namespace GaldrDbEngine.Query.Planning;

internal readonly struct CompoundScanKeys
{
    public byte[] StartKey { get; }
    public byte[] EndKey { get; }
    public byte[] PrefixKey { get; }
    public SecondaryIndexOperation Operation { get; }

    public CompoundScanKeys(
        byte[] startKey,
        byte[] endKey,
        byte[] prefixKey,
        SecondaryIndexOperation operation)
    {
        StartKey = startKey;
        EndKey = endKey;
        PrefixKey = prefixKey;
        Operation = operation;
    }
}
