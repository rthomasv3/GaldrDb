using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class QueryPlanner
{
    private const string ID_FIELD_NAME = "Id";

    private readonly CollectionEntry _collection;

    public QueryPlanner(CollectionEntry collection)
    {
        _collection = collection;
    }

    public QueryPlan CreatePlan(IReadOnlyList<IFieldFilter> filters)
    {
        QueryPlan result;

        if (filters.Count == 0)
        {
            result = QueryPlan.PrimaryKeyScan();
        }
        else
        {
            result = FindIdBasedPlan(filters);

            if (result == null)
            {
                IndexedFilterResult indexResult = FindBestIndexedFilter(filters);
                if (indexResult != null)
                {
                    result = QueryPlan.SecondaryIndex(indexResult.IndexDefinition, indexResult.Filter, indexResult.FilterIndex);
                }
                else
                {
                    result = QueryPlan.FullScan();
                }
            }
        }

        return result;
    }

    private QueryPlan FindIdBasedPlan(IReadOnlyList<IFieldFilter> filters)
    {
        QueryPlan result = null;

        for (int i = 0; i < filters.Count; i++)
        {
            IFieldFilter filter = filters[i];

            if (filter.FieldName != ID_FIELD_NAME)
            {
                continue;
            }

            if (!CanUseIdIndex(filter.Operation))
            {
                continue;
            }

            QueryPlan plan = CreateIdPlan(filter, i);
            if (plan != null)
            {
                result = plan;
                break;
            }
        }

        return result;
    }

    private QueryPlan CreateIdPlan(IFieldFilter filter, int filterIndex)
    {
        QueryPlan result = null;

        switch (filter.Operation)
        {
            case FieldOp.Equals:
                int eqValue = GetFilterIntValue(filter);
                result = QueryPlan.PrimaryKeyRange(eqValue, eqValue, true, true, filterIndex);
                break;

            case FieldOp.GreaterThan:
                int gtValue = GetFilterIntValue(filter);
                result = QueryPlan.PrimaryKeyRange(gtValue, int.MaxValue, false, true, filterIndex);
                break;

            case FieldOp.GreaterThanOrEqual:
                int gteValue = GetFilterIntValue(filter);
                result = QueryPlan.PrimaryKeyRange(gteValue, int.MaxValue, true, true, filterIndex);
                break;

            case FieldOp.LessThan:
                int ltValue = GetFilterIntValue(filter);
                result = QueryPlan.PrimaryKeyRange(int.MinValue, ltValue, true, false, filterIndex);
                break;

            case FieldOp.LessThanOrEqual:
                int lteValue = GetFilterIntValue(filter);
                result = QueryPlan.PrimaryKeyRange(int.MinValue, lteValue, true, true, filterIndex);
                break;

            case FieldOp.Between:
                result = CreateBetweenIdPlan(filter, filterIndex);
                break;

            case FieldOp.In:
                result = CreateInIdPlan(filter, filterIndex);
                break;
        }

        return result;
    }

    private static QueryPlan CreateInIdPlan(IFieldFilter filter, int filterIndex)
    {
        QueryPlan result = null;
        IReadOnlyList<int> docIds = filter.GetInValuesAsInt32();

        if (docIds != null && docIds.Count > 0)
        {
            result = QueryPlan.PrimaryKeyMultiPoint(docIds, filterIndex);
        }

        return result;
    }

    private QueryPlan CreateBetweenIdPlan(IFieldFilter filter, int filterIndex)
    {
        QueryPlan result = null;
        object minObj = filter.GetRangeMinValue();
        object maxObj = filter.GetRangeMaxValue();

        if (minObj is int minVal && maxObj is int maxVal)
        {
            result = QueryPlan.PrimaryKeyRange(minVal, maxVal, true, true, filterIndex);
        }

        return result;
    }

    private static int GetFilterIntValue(IFieldFilter filter)
    {
        int result;
        
        if (filter.GetFilterValue() is int intValue)
        {
            result = intValue;
        }
        else
        {
            throw new InvalidOperationException($"Cannot extract int value from filter on field '{filter.FieldName}'");
        }

        return result;
    }

    private static bool CanUseIdIndex(FieldOp op)
    {
        bool result;

        switch (op)
        {
            case FieldOp.Equals:
            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
            case FieldOp.Between:
            case FieldOp.In:
                result = true;
                break;
            default:
                result = false;
                break;
        }

        return result;
    }

    public IndexedFilterResult FindBestIndexedFilter(IReadOnlyList<IFieldFilter> filters)
    {
        return FindBestIndexedFilter(filters, Array.Empty<string>());
    }

    public IndexedFilterResult FindBestIndexedFilter(
        IReadOnlyList<IFieldFilter> filters,
        IReadOnlyList<string> orderByFieldNames)
    {
        IndexedFilterResult result = null;
        int bestScore = -1;
        int bestFilterIndex = -1;
        IndexDefinition bestIndexDef = null;

        // Check single-field indexes
        for (int i = 0; i < filters.Count; i++)
        {
            IFieldFilter filter = filters[i];

            if (!filter.IsIndexed)
            {
                continue;
            }

            if (!CanUseSecondaryIndex(filter.Operation))
            {
                continue;
            }

            IndexDefinition indexDef = _collection.FindIndex(filter.FieldName);
            if (indexDef == null)
            {
                continue;
            }

            int score = ScoreSingleFieldIndex(filter.Operation, filter.FieldName, orderByFieldNames);
            if (score > bestScore)
            {
                bestScore = score;
                bestFilterIndex = i;
                bestIndexDef = indexDef;
            }
        }

        // Check compound indexes
        CompoundIndexMatch compoundMatch = FindBestCompoundIndex(filters, orderByFieldNames);
        if (compoundMatch != null && compoundMatch.Score > bestScore)
        {
            IFieldFilter leadingFilter = filters[compoundMatch.LeadingFilterIndex];
            result = new IndexedFilterResult(
                leadingFilter,
                compoundMatch.IndexDefinition,
                compoundMatch.LeadingFilterIndex,
                compoundMatch.MatchedFilters,
                compoundMatch.MatchedFilterIndices,
                compoundMatch.EqualityFieldCount,
                compoundMatch.HasRangeField);
        }
        else if (bestFilterIndex >= 0 && bestIndexDef != null)
        {
            IFieldFilter bestFilter = filters[bestFilterIndex];
            result = new IndexedFilterResult(bestFilter, bestIndexDef, bestFilterIndex);
        }

        return result;
    }

    private static int ScoreSingleFieldIndex(
        FieldOp op,
        string fieldName,
        IReadOnlyList<string> orderByFieldNames)
    {
        int result;

        switch (op)
        {
            case FieldOp.Equals:
                result = 10;
                break;
            case FieldOp.In:
                result = 8;
                break;
            case FieldOp.StartsWith:
                result = 7;
                break;
            case FieldOp.Between:
                result = 5;
                break;
            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
                result = 5;
                break;
            default:
                result = 0;
                break;
        }

        // Add sort field bonus if index field matches ORDER BY
        if (orderByFieldNames.Count > 0 && orderByFieldNames[0] == fieldName)
        {
            result += 3;
        }

        return result;
    }

    private CompoundIndexMatch FindBestCompoundIndex(
        IReadOnlyList<IFieldFilter> filters,
        IReadOnlyList<string> orderByFieldNames)
    {
        CompoundIndexMatch result = null;
        int bestScore = 0;

        Dictionary<string, IFieldFilter> filtersByField = new Dictionary<string, IFieldFilter>();
        Dictionary<string, int> filterIndexByField = new Dictionary<string, int>();
        for (int i = 0; i < filters.Count; i++)
        {
            IFieldFilter filter = filters[i];
            if (CanUseSecondaryIndex(filter.Operation))
            {
                filtersByField[filter.FieldName] = filter;
                filterIndexByField[filter.FieldName] = i;
            }
        }

        foreach (IndexDefinition indexDef in _collection.Indexes)
        {
            if (!indexDef.IsCompound)
            {
                continue;
            }

            int equalityCount = 0;
            bool hasRange = false;
            int leadingFilterIndex = -1;
            List<IFieldFilter> matchedFilters = new List<IFieldFilter>();
            List<int> matchedFilterIndices = new List<int>();

            // Check leftmost-prefix matching
            for (int i = 0; i < indexDef.Fields.Count; i++)
            {
                IndexField field = indexDef.Fields[i];
                if (!filtersByField.TryGetValue(field.FieldName, out IFieldFilter filter))
                {
                    break;
                }

                int filterIndex = filterIndexByField[field.FieldName];
                if (i == 0)
                {
                    leadingFilterIndex = filterIndex;
                }

                matchedFilters.Add(filter);
                matchedFilterIndices.Add(filterIndex);

                if (filter.Operation == FieldOp.Equals || filter.Operation == FieldOp.In)
                {
                    equalityCount++;
                }
                else if (IsRangeOperation(filter.Operation))
                {
                    hasRange = true;
                    break;
                }
                else
                {
                    break;
                }
            }

            if (equalityCount > 0 || hasRange)
            {
                int score = (equalityCount * 10) + (hasRange ? 5 : 0);

                // Add sort field bonus: check if next index field matches ORDER BY
                int sortBonus = CalculateSortBonus(indexDef, matchedFilters.Count, orderByFieldNames);
                score += sortBonus;

                if (score > bestScore && leadingFilterIndex >= 0)
                {
                    bestScore = score;
                    result = new CompoundIndexMatch(
                        indexDef,
                        score,
                        equalityCount,
                        hasRange,
                        leadingFilterIndex,
                        matchedFilters,
                        matchedFilterIndices);
                }
            }
        }

        return result;
    }

    private static int CalculateSortBonus(
        IndexDefinition indexDef,
        int matchedFieldCount,
        IReadOnlyList<string> orderByFieldNames)
    {
        int bonus = 0;

        if (orderByFieldNames.Count > 0)
        {
            // Check if the first ORDER BY field matches the next index field after matched filters
            // or if it matches one of the matched equality fields (already sorted by equality)
            string orderByField = orderByFieldNames[0];

            // Case 1: ORDER BY field is the next field in the index after matched filters
            if (matchedFieldCount < indexDef.Fields.Count &&
                indexDef.Fields[matchedFieldCount].FieldName == orderByField)
            {
                bonus = 3;
            }
            // Case 2: ORDER BY field matches the first field in the index (already in order)
            else if (indexDef.Fields[0].FieldName == orderByField)
            {
                bonus = 3;
            }
        }

        return bonus;
    }

    private static bool IsRangeOperation(FieldOp op)
    {
        bool result;

        switch (op)
        {
            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
            case FieldOp.Between:
                result = true;
                break;
            default:
                result = false;
                break;
        }

        return result;
    }

    private static bool CanUseSecondaryIndex(FieldOp op)
    {
        bool result;

        switch (op)
        {
            case FieldOp.Equals:
            case FieldOp.In:
            case FieldOp.StartsWith:
            case FieldOp.Between:
            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
                result = true;
                break;
            default:
                // EndsWith, Contains, NotEquals, NotIn cannot efficiently use an index
                result = false;
                break;
        }

        return result;
    }

    private static int GetOperationPriority(FieldOp op)
    {
        int result;

        switch (op)
        {
            case FieldOp.Equals:
                result = 1;
                break;
            case FieldOp.In:
                result = 2;
                break;
            case FieldOp.StartsWith:
                result = 3;
                break;
            case FieldOp.Between:
                result = 4;
                break;
            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
                result = 5;
                break;
            default:
                result = 100;
                break;
        }

        return result;
    }

    public QueryExecutionPlan CreateExecutionPlan(
        IReadOnlyList<IFieldFilter> filters,
        IReadOnlyList<string> orderByFieldNames,
        bool hasDescendingOrder)
    {
        QueryExecutionPlan result;
        bool isOrderByIdOnly = IsOrderByIdOnly(orderByFieldNames);
        ScanDirection direction = hasDescendingOrder ? ScanDirection.Descending : ScanDirection.Ascending;

        if (filters.Count == 0)
        {
            result = QueryExecutionPlan.CreatePrimaryKeyScan(direction, isOrderByIdOnly, !isOrderByIdOnly);
        }
        else
        {
            QueryPlan basePlan = FindIdBasedPlan(filters);

            if (basePlan != null)
            {
                IReadOnlyList<IFieldFilter> remaining = ComputeRemainingFilters(filters, basePlan.UsedFilterIndex);
                bool canOptimize = isOrderByIdOnly && remaining.Count == 0;

                if (basePlan.PlanType == QueryPlanType.PrimaryKeyMultiPoint)
                {
                    PrimaryKeyMultiPointSpec multiPointSpec = new PrimaryKeyMultiPointSpec(basePlan.DocIds);
                    result = QueryExecutionPlan.CreatePrimaryKeyMultiPoint(
                        multiPointSpec,
                        basePlan.UsedFilterIndex.Value,
                        remaining,
                        direction,
                        canOptimize,
                        !isOrderByIdOnly);
                }
                else
                {
                    PrimaryKeyRangeSpec rangeSpec = new PrimaryKeyRangeSpec(
                        basePlan.StartDocId ?? int.MinValue,
                        basePlan.EndDocId ?? int.MaxValue,
                        basePlan.IncludeStart,
                        basePlan.IncludeEnd);
                    result = QueryExecutionPlan.CreatePrimaryKeyRange(
                        rangeSpec,
                        basePlan.UsedFilterIndex.Value,
                        remaining,
                        direction,
                        canOptimize,
                        !isOrderByIdOnly);
                }
            }
            else
            {
                IndexedFilterResult indexResult = FindBestIndexedFilter(filters, orderByFieldNames);
                if (indexResult != null)
                {
                    SecondaryIndexSpec indexSpec;
                    IReadOnlyList<IFieldFilter> remaining;

                    if (indexResult.IsCompoundIndex)
                    {
                        remaining = ComputeRemainingFiltersExcluding(filters, indexResult.MatchedFilterIndices);
                        CompoundScanKeys scanKeys = BuildCompoundScanKeys(indexResult);

                        indexSpec = new SecondaryIndexSpec(
                            indexResult.IndexDefinition,
                            indexResult.Filter,
                            scanKeys.Operation,
                            ScanDirection.Ascending,
                            indexResult.MatchedFilters,
                            scanKeys.StartKey,
                            scanKeys.EndKey,
                            scanKeys.PrefixKey);
                    }
                    else
                    {
                        remaining = ComputeRemainingFilters(filters, indexResult.FilterIndex);
                        SecondaryIndexOperation indexOp = GetSecondaryIndexOperation(indexResult.Filter.Operation);
                        string indexedFieldName = indexResult.Filter.FieldName;
                        bool orderMatchesIndex = orderByFieldNames.Count == 0 ||
                            (orderByFieldNames.Count == 1 && orderByFieldNames[0] == indexedFieldName);
                        ScanDirection indexDirection = orderMatchesIndex ? direction : ScanDirection.Ascending;

                        indexSpec = new SecondaryIndexSpec(
                            indexResult.IndexDefinition,
                            indexResult.Filter,
                            indexOp,
                            indexDirection);
                    }

                    string firstFieldName = indexResult.IndexDefinition.Fields[0].FieldName;
                    bool orderMatchesIndexField = orderByFieldNames.Count == 0 ||
                        (orderByFieldNames.Count == 1 && orderByFieldNames[0] == firstFieldName);
                    bool requiresPostScanOrdering = !orderMatchesIndexField;
                    bool canOptimize = remaining.Count == 0 && !requiresPostScanOrdering;

                    result = QueryExecutionPlan.CreateSecondaryIndexScan(
                        indexSpec,
                        indexResult.FilterIndex,
                        remaining,
                        canOptimize,
                        requiresPostScanOrdering);
                }
                else
                {
                    result = QueryExecutionPlan.CreateFullScan(filters, !isOrderByIdOnly);
                }
            }
        }

        return result;
    }

    private static bool IsOrderByIdOnly(IReadOnlyList<string> orderByFieldNames)
    {
        return orderByFieldNames.Count == 0 ||
               (orderByFieldNames.Count == 1 && orderByFieldNames[0] == ID_FIELD_NAME);
    }

    private static IReadOnlyList<IFieldFilter> ComputeRemainingFilters(IReadOnlyList<IFieldFilter> filters, int? usedFilterIndex)
    {
        IReadOnlyList<IFieldFilter> result;

        if (!usedFilterIndex.HasValue)
        {
            result = filters;
        }
        else
        {
            List<IFieldFilter> remaining = new List<IFieldFilter>(filters.Count - 1);
            for (int i = 0; i < filters.Count; i++)
            {
                if (i != usedFilterIndex.Value)
                {
                    remaining.Add(filters[i]);
                }
            }
            result = remaining;
        }

        return result;
    }

    private static IReadOnlyList<IFieldFilter> ComputeRemainingFiltersExcluding(
        IReadOnlyList<IFieldFilter> filters,
        IReadOnlyList<int> excludedIndices)
    {
        HashSet<int> excluded = new HashSet<int>(excludedIndices);
        List<IFieldFilter> result = new List<IFieldFilter>();

        for (int i = 0; i < filters.Count; i++)
        {
            if (!excluded.Contains(i))
            {
                result.Add(filters[i]);
            }
        }

        return result;
    }

    private static CompoundScanKeys BuildCompoundScanKeys(IndexedFilterResult indexResult)
    {
        IndexDefinition indexDef = indexResult.IndexDefinition;
        IReadOnlyList<IFieldFilter> matchedFilters = indexResult.MatchedFilters;
        int equalityCount = indexResult.EqualityFieldCount;
        bool hasRange = indexResult.HasRangeField;
        int matchedCount = matchedFilters.Count;

        object[] startValues = new object[matchedCount];
        object[] endValues = new object[matchedCount];
        GaldrFieldType[] fieldTypes = new GaldrFieldType[matchedCount];

        for (int i = 0; i < matchedCount; i++)
        {
            fieldTypes[i] = indexDef.Fields[i].FieldType;
        }

        bool isExactMatch = !hasRange && matchedCount == indexDef.Fields.Count;
        bool isPrefixOnly = !hasRange && matchedCount < indexDef.Fields.Count;
        SecondaryIndexOperation operation;
        byte[] startKey;
        byte[] endKey = null;
        byte[] prefixKey = null;

        if (isExactMatch)
        {
            for (int i = 0; i < matchedCount; i++)
            {
                startValues[i] = matchedFilters[i].GetFilterValue();
            }
            startKey = IndexKeyEncoder.EncodeCompound(startValues, fieldTypes);
            operation = SecondaryIndexOperation.ExactMatch;
        }
        else if (isPrefixOnly)
        {
            for (int i = 0; i < matchedCount; i++)
            {
                startValues[i] = matchedFilters[i].GetFilterValue();
            }
            startKey = IndexKeyEncoder.EncodeCompound(startValues, fieldTypes);
            operation = SecondaryIndexOperation.PrefixMatch;
        }
        else
        {
            for (int i = 0; i < equalityCount; i++)
            {
                startValues[i] = matchedFilters[i].GetFilterValue();
            }

            IFieldFilter rangeFilter = matchedFilters[matchedCount - 1];
            FieldOp rangeOp = rangeFilter.Operation;
            byte[] equalityPrefix = IndexKeyEncoder.EncodeCompound(startValues, fieldTypes, equalityCount);

            if (rangeOp == FieldOp.GreaterThan || rangeOp == FieldOp.GreaterThanOrEqual)
            {
                startValues[equalityCount] = rangeFilter.GetFilterValue();
                startKey = IndexKeyEncoder.EncodeCompound(startValues, fieldTypes, equalityCount + 1);
                prefixKey = equalityPrefix;
                operation = SecondaryIndexOperation.PrefixRangeScan;
            }
            else if (rangeOp == FieldOp.LessThan || rangeOp == FieldOp.LessThanOrEqual)
            {
                startKey = equalityPrefix;
                for (int i = 0; i < equalityCount; i++)
                {
                    endValues[i] = startValues[i];
                }
                endValues[equalityCount] = rangeFilter.GetFilterValue();
                endKey = IndexKeyEncoder.EncodeCompound(endValues, fieldTypes, equalityCount + 1);
                operation = SecondaryIndexOperation.RangeScan;
            }
            else if (rangeOp == FieldOp.Between)
            {
                startValues[equalityCount] = rangeFilter.GetRangeMinValue();
                startKey = IndexKeyEncoder.EncodeCompound(startValues, fieldTypes, equalityCount + 1);
                for (int i = 0; i < equalityCount; i++)
                {
                    endValues[i] = startValues[i];
                }
                endValues[equalityCount] = rangeFilter.GetRangeMaxValue();
                endKey = IndexKeyEncoder.EncodeCompound(endValues, fieldTypes, equalityCount + 1);
                operation = SecondaryIndexOperation.RangeScan;
            }
            else
            {
                startKey = equalityPrefix;
                operation = SecondaryIndexOperation.PrefixMatch;
            }
        }

        return new CompoundScanKeys(startKey, endKey, prefixKey, operation);
    }

    private static SecondaryIndexOperation GetSecondaryIndexOperation(FieldOp op)
    {
        SecondaryIndexOperation result;

        switch (op)
        {
            case FieldOp.Equals:
                result = SecondaryIndexOperation.ExactMatch;
                break;
            case FieldOp.StartsWith:
                result = SecondaryIndexOperation.PrefixMatch;
                break;
            case FieldOp.In:
                result = SecondaryIndexOperation.MultiMatch;
                break;
            default:
                result = SecondaryIndexOperation.RangeScan;
                break;
        }

        return result;
    }
}
