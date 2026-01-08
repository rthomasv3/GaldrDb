using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

internal sealed class AutoDisposingQueryExecutor<T> : IQueryExecutor<T>
{
    private readonly IQueryExecutor<T> _innerExecutor;
    private readonly IDisposable _resource;

    public AutoDisposingQueryExecutor(IQueryExecutor<T> innerExecutor, IDisposable resource)
    {
        _innerExecutor = innerExecutor;
        _resource = resource;
    }

    public List<T> ExecuteQuery(QueryBuilder<T> query)
    {
        try
        {
            return _innerExecutor.ExecuteQuery(query);
        }
        finally
        {
            _resource.Dispose();
        }
    }

    public int ExecuteCount(QueryBuilder<T> query)
    {
        try
        {
            return _innerExecutor.ExecuteCount(query);
        }
        finally
        {
            _resource.Dispose();
        }
    }

    public async Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _innerExecutor.ExecuteQueryAsync(query, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _resource.Dispose();
        }
    }

    public async Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _innerExecutor.ExecuteCountAsync(query, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _resource.Dispose();
        }
    }
}
