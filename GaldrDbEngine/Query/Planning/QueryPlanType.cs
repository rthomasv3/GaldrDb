namespace GaldrDbEngine.Query.Planning;

internal enum QueryPlanType
{
    FullScan,
    PrimaryKeyScan,
    PrimaryKeyRange,
    SecondaryIndexScan
}
