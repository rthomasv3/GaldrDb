using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

public sealed class DatabaseQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly GaldrDb _db;
    private readonly GaldrTypeInfo<T> _typeInfo;

    public DatabaseQueryExecutor(GaldrDb db, GaldrTypeInfo<T> typeInfo)
    {
        _db = db;
        _typeInfo = typeInfo;
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        List<T> results;

        CollectionEntry collection = _db.GetCollection(_typeInfo.CollectionName);
        if (collection == null)
        {
            results = new List<T>();
        }
        else if (collection.Indexes.Count == 0 || query.Filters.Count == 0)
        {
            results = ExecuteFullScan(query);
        }
        else
        {
            QueryPlanner planner = new QueryPlanner(collection);
            IndexedFilterResult indexedFilter = planner.FindBestIndexedFilter(query.Filters);

            if (indexedFilter == null)
            {
                results = ExecuteFullScan(query);
            }
            else
            {
                results = ExecuteIndexedQuery(query, collection, indexedFilter);
            }
        }

        return results;
    }

    private List<T> ExecuteFullScan(QueryBuilder<T> query)
    {
        List<T> allDocuments = _db.GetAllDocuments(_typeInfo);

        List<T> results = new List<T>();
        int skipped = 0;
        int skipCount = query.SkipValue ?? 0;
        int? limitCount = query.LimitValue;

        foreach (T document in allDocuments)
        {
            if (limitCount.HasValue && results.Count >= limitCount.Value)
            {
                break;
            }

            bool passesAllFilters = true;
            foreach (IFieldFilter filter in query.Filters)
            {
                if (!filter.Evaluate(document))
                {
                    passesAllFilters = false;
                    break;
                }
            }

            if (passesAllFilters)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else
                {
                    results.Add(document);
                }
            }
        }

        return results;
    }

    private List<T> ExecuteIndexedQuery(QueryBuilder<T> query, CollectionEntry collection, IndexedFilterResult indexedFilter)
    {
        List<DocumentLocation> candidateLocations = GetCandidateLocations(indexedFilter, collection);

        List<T> candidates = _db.GetDocumentsByLocations(candidateLocations, _typeInfo);

        List<IFieldFilter> remainingFilters = GetRemainingFilters(query.Filters, indexedFilter.FilterIndex);

        List<T> results = new List<T>();
        int skipped = 0;
        int skipCount = query.SkipValue ?? 0;
        int? limitCount = query.LimitValue;

        foreach (T document in candidates)
        {
            if (limitCount.HasValue && results.Count >= limitCount.Value)
            {
                break;
            }

            bool passesRemainingFilters = true;
            foreach (IFieldFilter filter in remainingFilters)
            {
                if (!filter.Evaluate(document))
                {
                    passesRemainingFilters = false;
                    break;
                }
            }

            if (passesRemainingFilters)
            {
                if (skipped < skipCount)
                {
                    skipped++;
                }
                else
                {
                    results.Add(document);
                }
            }
        }

        return results;
    }

    private List<DocumentLocation> GetCandidateLocations(IndexedFilterResult indexedFilter, CollectionEntry collection)
    {
        List<DocumentLocation> locations = new List<DocumentLocation>();
        SecondaryIndexBTree indexTree = _db.GetSecondaryIndexTree(indexedFilter.IndexDefinition);
        IFieldFilter filter = indexedFilter.Filter;

        switch (filter.Operation)
        {
            case FieldOp.Equals:
                locations = ExecuteEqualsLookup(indexTree, filter);
                break;

            case FieldOp.In:
                locations = ExecuteInLookup(indexTree, filter);
                break;

            case FieldOp.StartsWith:
                locations = ExecuteStartsWithLookup(indexTree, filter);
                break;

            case FieldOp.Between:
                locations = ExecuteBetweenLookup(indexTree, filter);
                break;

            case FieldOp.GreaterThan:
            case FieldOp.GreaterThanOrEqual:
            case FieldOp.LessThan:
            case FieldOp.LessThanOrEqual:
                locations = ExecuteRangeLookup(indexTree, filter);
                break;

            default:
                locations = GetAllLocationsFromIndex(indexTree);
                break;
        }

        return locations;
    }

    private List<DocumentLocation> ExecuteEqualsLookup(SecondaryIndexBTree indexTree, IFieldFilter filter)
    {
        byte[] keyBytes = filter.GetIndexKeyBytes();
        List<DocumentLocation> results = indexTree.SearchByFieldValue(keyBytes);
        return results;
    }

    private List<DocumentLocation> ExecuteInLookup(SecondaryIndexBTree indexTree, IFieldFilter filter)
    {
        List<DocumentLocation> results = new List<DocumentLocation>();

        if (filter is InFilter<T, string> stringInFilter)
        {
            foreach (byte[] keyBytes in stringInFilter.GetAllIndexKeyBytes())
            {
                List<DocumentLocation> matches = indexTree.SearchByFieldValue(keyBytes);
                results.AddRange(matches);
            }
        }
        else if (filter is InFilter<T, int> intInFilter)
        {
            foreach (byte[] keyBytes in intInFilter.GetAllIndexKeyBytes())
            {
                List<DocumentLocation> matches = indexTree.SearchByFieldValue(keyBytes);
                results.AddRange(matches);
            }
        }
        else
        {
            byte[] keyBytes = filter.GetIndexKeyBytes();
            results = indexTree.SearchByFieldValue(keyBytes);
        }

        return results;
    }

    private List<DocumentLocation> ExecuteStartsWithLookup(SecondaryIndexBTree indexTree, IFieldFilter filter)
    {
        byte[] startKey = filter.GetIndexKeyBytes();
        byte[] endKey = filter.GetIndexKeyEndBytes();

        List<DocumentLocation> results;
        if (endKey != null)
        {
            results = indexTree.SearchRange(startKey, endKey, true, false);
        }
        else
        {
            results = indexTree.SearchByFieldValue(startKey);
        }

        return results;
    }

    private List<DocumentLocation> ExecuteBetweenLookup(SecondaryIndexBTree indexTree, IFieldFilter filter)
    {
        byte[] startKey = filter.GetIndexKeyBytes();
        byte[] endKey = filter.GetIndexKeyEndBytes();

        List<DocumentLocation> results = indexTree.SearchRange(startKey, endKey, true, true);
        return results;
    }

    private List<DocumentLocation> ExecuteRangeLookup(SecondaryIndexBTree indexTree, IFieldFilter filter)
    {
        byte[] keyBytes = filter.GetIndexKeyBytes();
        List<DocumentLocation> results = new List<DocumentLocation>();

        switch (filter.Operation)
        {
            case FieldOp.GreaterThan:
                results = indexTree.SearchRange(keyBytes, null, false, true);
                break;

            case FieldOp.GreaterThanOrEqual:
                results = indexTree.SearchRange(keyBytes, null, true, true);
                break;

            case FieldOp.LessThan:
                results = indexTree.SearchRange(null, keyBytes, true, false);
                break;

            case FieldOp.LessThanOrEqual:
                results = indexTree.SearchRange(null, keyBytes, true, true);
                break;
        }

        return results;
    }

    private List<DocumentLocation> GetAllLocationsFromIndex(SecondaryIndexBTree indexTree)
    {
        List<DocumentLocation> results = indexTree.SearchRange(null, null, true, true);
        return results;
    }

    private List<IFieldFilter> GetRemainingFilters(IReadOnlyList<IFieldFilter> filters, int excludeIndex)
    {
        List<IFieldFilter> remaining = new List<IFieldFilter>();

        for (int i = 0; i < filters.Count; i++)
        {
            if (i != excludeIndex)
            {
                remaining.Add(filters[i]);
            }
        }

        return remaining;
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        int count;

        CollectionEntry collection = _db.GetCollection(_typeInfo.CollectionName);
        if (collection == null)
        {
            count = 0;
        }
        else if (collection.Indexes.Count == 0 || query.Filters.Count == 0)
        {
            count = ExecuteFullScanCount(query);
        }
        else
        {
            QueryPlanner planner = new QueryPlanner(collection);
            IndexedFilterResult indexedFilter = planner.FindBestIndexedFilter(query.Filters);

            if (indexedFilter == null)
            {
                count = ExecuteFullScanCount(query);
            }
            else
            {
                count = ExecuteIndexedCount(query, collection, indexedFilter);
            }
        }

        return count;
    }

    private int ExecuteFullScanCount(QueryBuilder<T> query)
    {
        List<T> allDocuments = _db.GetAllDocuments(_typeInfo);

        int count = 0;

        foreach (T document in allDocuments)
        {
            bool passesAllFilters = true;
            foreach (IFieldFilter filter in query.Filters)
            {
                if (!filter.Evaluate(document))
                {
                    passesAllFilters = false;
                    break;
                }
            }

            if (passesAllFilters)
            {
                count++;
            }
        }

        return count;
    }

    private int ExecuteIndexedCount(QueryBuilder<T> query, CollectionEntry collection, IndexedFilterResult indexedFilter)
    {
        List<IFieldFilter> remainingFilters = GetRemainingFilters(query.Filters, indexedFilter.FilterIndex);

        int count;
        if (remainingFilters.Count == 0)
        {
            List<DocumentLocation> locations = GetCandidateLocations(indexedFilter, collection);
            count = locations.Count;
        }
        else
        {
            List<DocumentLocation> candidateLocations = GetCandidateLocations(indexedFilter, collection);
            List<T> candidates = _db.GetDocumentsByLocations(candidateLocations, _typeInfo);

            count = 0;
            foreach (T document in candidates)
            {
                bool passesRemainingFilters = true;
                foreach (IFieldFilter filter in remainingFilters)
                {
                    if (!filter.Evaluate(document))
                    {
                        passesRemainingFilters = false;
                        break;
                    }
                }

                if (passesRemainingFilters)
                {
                    count++;
                }
            }
        }

        return count;
    }
}
