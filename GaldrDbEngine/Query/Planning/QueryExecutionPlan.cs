using System.Collections.Generic;

namespace GaldrDbEngine.Query.Planning;

internal sealed class QueryExecutionPlan
{
    public QueryPlanType PlanType { get; }
    public int? UsedFilterIndex { get; }
    public IReadOnlyList<IFieldFilter> RemainingFilters { get; }
    public PrimaryKeyRangeSpec PrimaryKeyRange { get; }
    public SecondaryIndexSpec SecondaryIndex { get; }
    public bool CanApplySkipLimitDuringScan { get; }
    public bool RequiresPostScanOrdering { get; }
    public ScanDirection ScanDirection { get; }

    private QueryExecutionPlan(
        QueryPlanType planType,
        int? usedFilterIndex,
        IReadOnlyList<IFieldFilter> remainingFilters,
        PrimaryKeyRangeSpec primaryKeyRange,
        SecondaryIndexSpec secondaryIndex,
        bool canApplySkipLimitDuringScan,
        bool requiresPostScanOrdering,
        ScanDirection scanDirection)
    {
        PlanType = planType;
        UsedFilterIndex = usedFilterIndex;
        RemainingFilters = remainingFilters;
        PrimaryKeyRange = primaryKeyRange;
        SecondaryIndex = secondaryIndex;
        CanApplySkipLimitDuringScan = canApplySkipLimitDuringScan;
        RequiresPostScanOrdering = requiresPostScanOrdering;
        ScanDirection = scanDirection;
    }

    public static QueryExecutionPlan CreateFullScan(
        IReadOnlyList<IFieldFilter> filters,
        bool requiresPostScanOrdering)
    {
        return new QueryExecutionPlan(
            QueryPlanType.FullScan,
            null,
            filters,
            null,
            null,
            false,
            requiresPostScanOrdering,
            ScanDirection.Ascending);
    }

    public static QueryExecutionPlan CreatePrimaryKeyScan(
        ScanDirection direction,
        bool canApplySkipLimitDuringScan,
        bool requiresPostScanOrdering)
    {
        return new QueryExecutionPlan(
            QueryPlanType.PrimaryKeyScan,
            null,
            EmptyFilters,
            null,
            null,
            canApplySkipLimitDuringScan,
            requiresPostScanOrdering,
            direction);
    }

    public static QueryExecutionPlan CreatePrimaryKeyRange(
        PrimaryKeyRangeSpec rangeSpec,
        int usedFilterIndex,
        IReadOnlyList<IFieldFilter> remainingFilters,
        ScanDirection direction,
        bool canApplySkipLimitDuringScan,
        bool requiresPostScanOrdering)
    {
        return new QueryExecutionPlan(
            QueryPlanType.PrimaryKeyRange,
            usedFilterIndex,
            remainingFilters,
            rangeSpec,
            null,
            canApplySkipLimitDuringScan,
            requiresPostScanOrdering,
            direction);
    }

    public static QueryExecutionPlan CreateSecondaryIndexScan(
        SecondaryIndexSpec indexSpec,
        int usedFilterIndex,
        IReadOnlyList<IFieldFilter> remainingFilters,
        bool requiresPostScanOrdering)
    {
        return new QueryExecutionPlan(
            QueryPlanType.SecondaryIndexScan,
            usedFilterIndex,
            remainingFilters,
            null,
            indexSpec,
            false,
            requiresPostScanOrdering,
            ScanDirection.Ascending);
    }

    private static readonly IReadOnlyList<IFieldFilter> EmptyFilters = new List<IFieldFilter>();
}
