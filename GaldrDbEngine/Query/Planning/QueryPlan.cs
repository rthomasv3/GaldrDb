using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class QueryPlan
{
    public QueryPlanType PlanType { get; }
    public int? UsedFilterIndex { get; }

    // For PrimaryKeyRange
    public int? StartDocId { get; }
    public int? EndDocId { get; }
    public bool IncludeStart { get; }
    public bool IncludeEnd { get; }

    // For SecondaryIndexScan
    public IndexDefinition IndexDefinition { get; }
    public IFieldFilter IndexFilter { get; }

    private QueryPlan(QueryPlanType planType)
    {
        PlanType = planType;
        IncludeStart = true;
        IncludeEnd = true;
    }

    private QueryPlan(
        QueryPlanType planType,
        int? usedFilterIndex,
        int? startDocId,
        int? endDocId,
        bool includeStart,
        bool includeEnd,
        IndexDefinition indexDefinition,
        IFieldFilter indexFilter)
    {
        PlanType = planType;
        UsedFilterIndex = usedFilterIndex;
        StartDocId = startDocId;
        EndDocId = endDocId;
        IncludeStart = includeStart;
        IncludeEnd = includeEnd;
        IndexDefinition = indexDefinition;
        IndexFilter = indexFilter;
    }

    public static QueryPlan FullScan()
    {
        return new QueryPlan(QueryPlanType.FullScan);
    }

    public static QueryPlan PrimaryKeyScan()
    {
        return new QueryPlan(QueryPlanType.PrimaryKeyScan);
    }

    public static QueryPlan PrimaryKeyRange(int? startDocId, int? endDocId, bool includeStart, bool includeEnd, int filterIndex)
    {
        return new QueryPlan(
            QueryPlanType.PrimaryKeyRange,
            filterIndex,
            startDocId,
            endDocId,
            includeStart,
            includeEnd,
            null,
            null);
    }

    public static QueryPlan SecondaryIndex(IndexDefinition indexDef, IFieldFilter filter, int filterIndex)
    {
        return new QueryPlan(
            QueryPlanType.SecondaryIndexScan,
            filterIndex,
            null,
            null,
            true,
            true,
            indexDef,
            filter);
    }
}
