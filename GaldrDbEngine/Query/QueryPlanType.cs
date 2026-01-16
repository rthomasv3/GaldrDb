namespace GaldrDbEngine.Query;

internal enum QueryPlanType
{
    FullScan,
    PrimaryKeyRange,
    SecondaryIndexScan
}
