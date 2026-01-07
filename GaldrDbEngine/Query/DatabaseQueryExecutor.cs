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
                    continue;
                }
                results.Add(document);
            }
        }

        return results;
    }

    public int ExecuteCount(QueryBuilder<T> query)
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
}
