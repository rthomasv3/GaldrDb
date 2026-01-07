using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

public sealed class QueryPlanner
{
    private readonly CollectionEntry _collection;

    public QueryPlanner(CollectionEntry collection)
    {
        _collection = collection;
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

            if (!CanUseIndex(filter.Operation))
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

    private static bool CanUseIndex(FieldOp op)
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
