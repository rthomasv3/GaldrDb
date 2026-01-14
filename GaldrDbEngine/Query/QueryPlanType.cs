namespace GaldrDbEngine.Query;

public enum QueryPlanType
{
    FullScan,
    PrimaryKeyRange,
    SecondaryIndexScan
}
