namespace GaldrDbEngine.Query;

public sealed class QueryExplanation
{
    public QueryScanType ScanType { get; }
    public string ScanDescription { get; }
    public string IndexedField { get; }
    public string RangeStart { get; }
    public string RangeEnd { get; }
    public bool IncludesStart { get; }
    public bool IncludesEnd { get; }
    public int TotalFilters { get; }
    public int FiltersUsedByIndex { get; }
    public int FiltersAppliedAfterScan { get; }

    private QueryExplanation(
        QueryScanType scanType,
        string scanDescription,
        string indexedField,
        string rangeStart,
        string rangeEnd,
        bool includesStart,
        bool includesEnd,
        int totalFilters,
        int filtersUsedByIndex)
    {
        ScanType = scanType;
        ScanDescription = scanDescription;
        IndexedField = indexedField;
        RangeStart = rangeStart;
        RangeEnd = rangeEnd;
        IncludesStart = includesStart;
        IncludesEnd = includesEnd;
        TotalFilters = totalFilters;
        FiltersUsedByIndex = filtersUsedByIndex;
        FiltersAppliedAfterScan = totalFilters - filtersUsedByIndex;
    }

    internal static QueryExplanation FromPlan(QueryPlan plan, int totalFilters)
    {
        QueryExplanation result;

        switch (plan.PlanType)
        {
            case QueryPlanType.PrimaryKeyRange:
                result = CreatePrimaryKeyRangeExplanation(plan, totalFilters);
                break;

            case QueryPlanType.SecondaryIndexScan:
                result = CreateSecondaryIndexExplanation(plan, totalFilters);
                break;

            default:
                result = new QueryExplanation(
                    QueryScanType.FullScan,
                    "Full collection scan - no index optimization available",
                    null,
                    null,
                    null,
                    false,
                    false,
                    totalFilters,
                    0);
                break;
        }

        return result;
    }

    private static QueryExplanation CreatePrimaryKeyRangeExplanation(QueryPlan plan, int totalFilters)
    {
        string rangeStart = FormatBound(plan.StartDocId, int.MinValue, "MIN");
        string rangeEnd = FormatBound(plan.EndDocId, int.MaxValue, "MAX");

        string description;
        if (plan.StartDocId == plan.EndDocId && plan.IncludeStart && plan.IncludeEnd)
        {
            description = $"Primary key lookup: Id = {plan.StartDocId}";
        }
        else
        {
            string startOp = plan.IncludeStart ? ">=" : ">";
            string endOp = plan.IncludeEnd ? "<=" : "<";
            description = $"Primary key range scan: Id {startOp} {rangeStart} AND Id {endOp} {rangeEnd}";
        }

        return new QueryExplanation(
            QueryScanType.PrimaryKeyRange,
            description,
            "Id",
            rangeStart,
            rangeEnd,
            plan.IncludeStart,
            plan.IncludeEnd,
            totalFilters,
            1);
    }

    private static QueryExplanation CreateSecondaryIndexExplanation(QueryPlan plan, int totalFilters)
    {
        string fieldName = plan.IndexFilter?.FieldName ?? "Unknown";
        string operation = plan.IndexFilter?.Operation.ToString() ?? "Unknown";
        string description = $"Secondary index scan on '{fieldName}' using {operation}";

        return new QueryExplanation(
            QueryScanType.SecondaryIndex,
            description,
            fieldName,
            null,
            null,
            true,
            true,
            totalFilters,
            1);
    }

    private static string FormatBound(int? value, int unboundedValue, string unboundedLabel)
    {
        string result;

        if (!value.HasValue || value.Value == unboundedValue)
        {
            result = unboundedLabel;
        }
        else
        {
            result = value.Value.ToString();
        }

        return result;
    }

    public override string ToString()
    {
        return ScanDescription;
    }
}
