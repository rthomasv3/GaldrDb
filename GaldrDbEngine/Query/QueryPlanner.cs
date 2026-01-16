using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

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
        QueryPlan result = FindIdBasedPlan(filters);

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
        }

        return result;
    }

    private QueryPlan CreateBetweenIdPlan(IFieldFilter filter, int filterIndex)
    {
        QueryPlan result = null;
        Type filterType = filter.GetType();

        if (filterType.IsGenericType)
        {
            System.Reflection.PropertyInfo minProp = filterType.GetProperty("MinValue");
            System.Reflection.PropertyInfo maxProp = filterType.GetProperty("MaxValue");

            if (minProp != null && maxProp != null)
            {
                object minObj = minProp.GetValue(filter);
                object maxObj = maxProp.GetValue(filter);

                if (minObj is int minVal && maxObj is int maxVal)
                {
                    result = QueryPlan.PrimaryKeyRange(minVal, maxVal, true, true, filterIndex);
                }
            }
        }

        return result;
    }

    private int GetFilterIntValue(IFieldFilter filter)
    {
        Type filterType = filter.GetType();
        System.Reflection.PropertyInfo valueProp = filterType.GetProperty("Value");

        if (valueProp != null)
        {
            object valueObj = valueProp.GetValue(filter);
            if (valueObj is int intValue)
            {
                return intValue;
            }
        }

        throw new InvalidOperationException($"Cannot extract int value from filter on field '{filter.FieldName}'");
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
        IndexedFilterResult result = null;
        int bestPriority = int.MaxValue;
        int bestFilterIndex = -1;

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

            int priority = GetOperationPriority(filter.Operation);
            if (priority < bestPriority)
            {
                bestPriority = priority;
                bestFilterIndex = i;
            }
        }

        if (bestFilterIndex >= 0)
        {
            IFieldFilter bestFilter = filters[bestFilterIndex];
            IndexDefinition indexDef = _collection.FindIndex(bestFilter.FieldName);

            result = new IndexedFilterResult(bestFilter, indexDef, bestFilterIndex);
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
}
