using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

/// <summary>
/// Builds queries for filtering and retrieving documents from a collection.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class QueryBuilder<T>
{
    private readonly List<IFieldFilter> _filters;
    private readonly List<OrderByClause<T>> _orderByClauses;
    private readonly IQueryExecutor<T> _executor;
    private int? _limit;
    private int? _skip;

    /// <summary>
    /// Creates a new query builder.
    /// </summary>
    /// <param name="executor">The query executor to use.</param>
    public QueryBuilder(IQueryExecutor<T> executor)
    {
        _executor = executor;
        _filters = new List<IFieldFilter>();
        _orderByClauses = new List<OrderByClause<T>>();
    }

    /// <summary>
    /// Adds a filter condition to the query.
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> Where<TField>(GaldrField<T, TField> field, FieldOp op, TField value)
    {
        _filters.Add(new FieldFilter<T, TField>(field, op, value));
        return this;
    }

    /// <summary>
    /// Adds a between filter (inclusive on both ends).
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> WhereBetween<TField>(GaldrField<T, TField> field, TField minValue, TField maxValue)
        where TField : IComparable<TField>
    {
        _filters.Add(new BetweenFilter<T, TField>(field, minValue, maxValue));
        return this;
    }

    /// <summary>
    /// Adds an IN filter to match any of the specified values.
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to filter on.</param>
    /// <param name="values">The values to match against.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> WhereIn<TField>(GaldrField<T, TField> field, params TField[] values)
    {
        _filters.Add(new InFilter<T, TField>(field, values));
        return this;
    }

    /// <summary>
    /// Adds a NOT IN filter to exclude the specified values.
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to filter on.</param>
    /// <param name="values">The values to exclude.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> WhereNotIn<TField>(GaldrField<T, TField> field, params TField[] values)
    {
        _filters.Add(new NotInFilter<T, TField>(field, values));
        return this;
    }

    internal void AddFilter(IFieldFilter filter)
    {
        _filters.Add(filter);
    }

    /// <summary>
    /// Limits the number of results returned.
    /// </summary>
    /// <param name="count">Maximum number of documents to return.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> Limit(int count)
    {
        _limit = count;
        return this;
    }

    /// <summary>
    /// Skips a number of results before returning.
    /// </summary>
    /// <param name="count">Number of documents to skip.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> Skip(int count)
    {
        _skip = count;
        return this;
    }

    /// <summary>
    /// Sorts results by a field in ascending order.
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to sort by.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> OrderBy<TField>(GaldrField<T, TField> field) where TField : IComparable<TField>
    {
        Comparison<T> comparer = (a, b) =>
        {
            TField valueA = field.Accessor(a);
            TField valueB = field.Accessor(b);

            if (valueA == null && valueB == null)
            {
                return 0;
            }
            if (valueA == null)
            {
                return -1;
            }
            if (valueB == null)
            {
                return 1;
            }

            return valueA.CompareTo(valueB);
        };

        _orderByClauses.Add(new OrderByClause<T>(field.FieldName, false, comparer));
        return this;
    }

    /// <summary>
    /// Sorts results by a field in descending order.
    /// </summary>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="field">The field to sort by.</param>
    /// <returns>This query builder for chaining.</returns>
    public QueryBuilder<T> OrderByDescending<TField>(GaldrField<T, TField> field) where TField : IComparable<TField>
    {
        Comparison<T> comparer = (a, b) =>
        {
            TField valueA = field.Accessor(a);
            TField valueB = field.Accessor(b);

            if (valueA == null && valueB == null)
            {
                return 0;
            }
            if (valueA == null)
            {
                return 1;
            }
            if (valueB == null)
            {
                return -1;
            }

            return valueB.CompareTo(valueA);
        };

        _orderByClauses.Add(new OrderByClause<T>(field.FieldName, true, comparer));
        return this;
    }

    /// <summary>
    /// Executes the query and returns all matching documents.
    /// </summary>
    /// <returns>List of matching documents.</returns>
    public List<T> ToList()
    {
        return _executor.ExecuteQuery(this);
    }

    /// <summary>
    /// Executes the query and returns the first matching document, or default if none.
    /// </summary>
    /// <returns>The first matching document, or default(T).</returns>
    public T FirstOrDefault()
    {
        int? originalLimit = _limit;
        _limit = 1;

        List<T> results = ToList();

        _limit = originalLimit;

        T result;
        if (results.Count > 0)
        {
            result = results[0];
        }
        else
        {
            result = default;
        }

        return result;
    }

    /// <summary>
    /// Executes the query and returns the count of matching documents.
    /// </summary>
    /// <returns>Number of matching documents.</returns>
    public int Count()
    {
        return _executor.ExecuteCount(this);
    }

    /// <summary>
    /// Gets the query execution plan without executing the query.
    /// </summary>
    /// <returns>Explanation of how the query would be executed.</returns>
    public QueryExplanation Explain()
    {
        return _executor.GetQueryExplanation(_filters);
    }

    /// <summary>
    /// Executes the query asynchronously and returns all matching documents.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of matching documents.</returns>
    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return _executor.ExecuteQueryAsync(this, cancellationToken);
    }

    /// <summary>
    /// Executes the query asynchronously and returns the first matching document.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The first matching document, or default(T).</returns>
    public async Task<T> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        int? originalLimit = _limit;
        _limit = 1;

        List<T> results = await _executor.ExecuteQueryAsync(this, cancellationToken).ConfigureAwait(false);

        _limit = originalLimit;

        if (results.Count > 0)
        {
            return results[0];
        }

        return default;
    }

    /// <summary>
    /// Executes the count query asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of matching documents.</returns>
    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        return _executor.ExecuteCountAsync(this, cancellationToken);
    }

    /// <summary>The list of filters applied to this query.</summary>
    public IReadOnlyList<IFieldFilter> Filters
    {
        get { return _filters; }
    }

    /// <summary>The maximum number of results to return, or null for unlimited.</summary>
    public int? LimitValue
    {
        get { return _limit; }
    }

    /// <summary>The number of results to skip, or null for none.</summary>
    public int? SkipValue
    {
        get { return _skip; }
    }

    /// <summary>The ordering clauses applied to this query.</summary>
    public IReadOnlyList<OrderByClause<T>> OrderByClauses
    {
        get { return _orderByClauses; }
    }

    internal IQueryExecutor<T> GetExecutor()
    {
        return _executor;
    }
}
