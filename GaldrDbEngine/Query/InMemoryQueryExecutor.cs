using System.Collections.Generic;

namespace GaldrDbCore.Query;

public sealed class InMemoryQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly IEnumerable<T> _documents;

    public InMemoryQueryExecutor(IEnumerable<T> documents)
    {
        _documents = documents;
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        List<T> results = new List<T>();
        int skipped = 0;
        int skipCount = query.SkipValue ?? 0;
        int? limitCount = query.LimitValue;

        foreach (T document in _documents)
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
                    continue;
                }
                results.Add(document);
            }
        }

        return results;
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        int count = 0;

        foreach (T document in _documents)
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
}
