namespace GaldrDbEngine.Query.Planning;

internal enum SecondaryIndexOperation
{
    ExactMatch,
    PrefixMatch,
    MultiMatch,
    RangeScan
}
