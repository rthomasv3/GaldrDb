using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

public sealed class QueryBuilder<T>
{
    private readonly List<IFieldFilter> _filters;
    private readonly List<OrderByClause<T>> _orderByClauses;
    private readonly IQueryExecutor<T> _executor;
    private int? _limit;
    private int? _skip;

    public QueryBuilder(IQueryExecutor<T> executor)
    {
        _executor = executor;
        _filters = new List<IFieldFilter>();
        _orderByClauses = new List<OrderByClause<T>>();
    }

    public QueryBuilder<T> Where<TField>(GaldrField<T, TField> field, FieldOp op, TField value)
    {
        _filters.Add(new FieldFilter<T, TField>(field, op, value));
        return this;
    }

    public QueryBuilder<T> WhereBetween<TField>(GaldrField<T, TField> field, TField minValue, TField maxValue)
        where TField : IComparable<TField>
    {
        _filters.Add(new BetweenFilter<T, TField>(field, minValue, maxValue));
        return this;
    }

    public QueryBuilder<T> WhereIn<TField>(GaldrField<T, TField> field, params TField[] values)
    {
        _filters.Add(new InFilter<T, TField>(field, values));
        return this;
    }

    public QueryBuilder<T> WhereNotIn<TField>(GaldrField<T, TField> field, params TField[] values)
    {
        _filters.Add(new NotInFilter<T, TField>(field, values));
        return this;
    }

    internal void AddFilter(IFieldFilter filter)
    {
        _filters.Add(filter);
    }

    public QueryBuilder<T> Limit(int count)
    {
        _limit = count;
        return this;
    }

    public QueryBuilder<T> Skip(int count)
    {
        _skip = count;
        return this;
    }

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

    public List<T> ToList()
    {
        return _executor.ExecuteQuery(this);
    }

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

    public int Count()
    {
        return _executor.ExecuteCount(this);
    }

    public Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return _executor.ExecuteQueryAsync(this, cancellationToken);
    }

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

    public Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        return _executor.ExecuteCountAsync(this, cancellationToken);
    }

    public IReadOnlyList<IFieldFilter> Filters
    {
        get { return _filters; }
    }

    public int? LimitValue
    {
        get { return _limit; }
    }

    public int? SkipValue
    {
        get { return _skip; }
    }

    public IReadOnlyList<OrderByClause<T>> OrderByClauses
    {
        get { return _orderByClauses; }
    }

    internal IQueryExecutor<T> GetExecutor()
    {
        return _executor;
    }
}
