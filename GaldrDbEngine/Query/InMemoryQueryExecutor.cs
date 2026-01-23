using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

/// <summary>
/// A query executor that operates on an in-memory collection of documents.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class InMemoryQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly IEnumerable<T> _documents;

    /// <summary>
    /// Creates a new in-memory query executor.
    /// </summary>
    /// <param name="documents">The collection of documents to query.</param>
    public InMemoryQueryExecutor(IEnumerable<T> documents)
    {
        _documents = documents;
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    public Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(ExecuteQuery(query));
    }

    /// <inheritdoc/>
    public Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(ExecuteCount(query));
    }

    /// <inheritdoc/>
    public bool ExecuteAny(QueryBuilder<T> query)
    {
        bool found = false;

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
                found = true;
                break;
            }
        }

        return found;
    }

    /// <inheritdoc/>
    public Task<bool> ExecuteAnyAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(ExecuteAny(query));
    }

    /// <inheritdoc/>
    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        QueryPlan plan = QueryPlan.FullScan();
        return QueryExplanation.FromPlan(plan, filters.Count);
    }
}
