namespace GaldrDbEngine.Query;

internal enum QueryPlanType
{
    FullScan,
    PrimaryKeyScan,
    PrimaryKeyRange,
    SecondaryIndexScan
}
