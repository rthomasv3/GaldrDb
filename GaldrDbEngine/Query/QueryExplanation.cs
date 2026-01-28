using GaldrDbEngine.Query.Planning;

namespace GaldrDbEngine.Query;

/// <summary>
/// Provides information about how a query will be executed.
/// </summary>
public sealed class QueryExplanation
{
    /// <summary>The type of scan that will be used.</summary>
    public QueryScanType ScanType { get; }

    /// <summary>A human-readable description of the scan strategy.</summary>
    public string ScanDescription { get; }

    /// <summary>The indexed field being used, if any.</summary>
    public string IndexedField { get; }

    /// <summary>The start of the range being scanned, if applicable.</summary>
    public string RangeStart { get; }

    /// <summary>The end of the range being scanned, if applicable.</summary>
    public string RangeEnd { get; }

    /// <summary>Whether the range start is inclusive.</summary>
    public bool IncludesStart { get; }

    /// <summary>Whether the range end is inclusive.</summary>
    public bool IncludesEnd { get; }

    /// <summary>Total number of filters in the query.</summary>
    public int TotalFilters { get; }

    /// <summary>Number of filters that can be satisfied by the index.</summary>
    public int FiltersUsedByIndex { get; }

    /// <summary>Number of filters that must be applied after the scan.</summary>
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

    internal static QueryExplanation FromExecutionPlan(QueryExecutionPlan plan, int totalFilters)
    {
        QueryExplanation result;

        switch (plan.PlanType)
        {
            case QueryPlanType.PrimaryKeyRange:
                result = CreatePrimaryKeyRangeExplanationFromExecutionPlan(plan, totalFilters);
                break;

            case QueryPlanType.SecondaryIndexScan:
                result = CreateSecondaryIndexExplanationFromExecutionPlan(plan, totalFilters);
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

    private static QueryExplanation CreatePrimaryKeyRangeExplanationFromExecutionPlan(QueryExecutionPlan plan, int totalFilters)
    {
        PrimaryKeyRangeSpec rangeSpec = plan.PrimaryKeyRange;
        string rangeStart = FormatBound(rangeSpec.StartDocId, int.MinValue, "MIN");
        string rangeEnd = FormatBound(rangeSpec.EndDocId, int.MaxValue, "MAX");

        string description;
        if (rangeSpec.StartDocId == rangeSpec.EndDocId && rangeSpec.IncludeStart && rangeSpec.IncludeEnd)
        {
            description = $"Primary key lookup: Id = {rangeSpec.StartDocId}";
        }
        else
        {
            string startOp = rangeSpec.IncludeStart ? ">=" : ">";
            string endOp = rangeSpec.IncludeEnd ? "<=" : "<";
            description = $"Primary key range scan: Id {startOp} {rangeStart} AND Id {endOp} {rangeEnd}";
        }

        return new QueryExplanation(
            QueryScanType.PrimaryKeyRange,
            description,
            "Id",
            rangeStart,
            rangeEnd,
            rangeSpec.IncludeStart,
            rangeSpec.IncludeEnd,
            totalFilters,
            1);
    }

    private static QueryExplanation CreateSecondaryIndexExplanationFromExecutionPlan(QueryExecutionPlan plan, int totalFilters)
    {
        SecondaryIndexSpec indexSpec = plan.SecondaryIndex;
        string fieldName = indexSpec?.IndexDefinition?.FieldName ?? "Unknown";
        string operation = indexSpec?.IndexFilter?.Operation.ToString() ?? "Unknown";
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

    /// <inheritdoc/>
    public override string ToString()
    {
        return ScanDescription;
    }
}
