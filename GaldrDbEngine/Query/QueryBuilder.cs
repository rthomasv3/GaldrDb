using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public sealed class QueryBuilder<T>
{
    private readonly List<IFieldFilter> _filters;
    private readonly IQueryExecutor<T> _executor;
    private int? _limit;
    private int? _skip;

    public QueryBuilder(IQueryExecutor<T> executor)
    {
        _executor = executor;
        _filters = new List<IFieldFilter>();
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
}
