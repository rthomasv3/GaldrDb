using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query.Execution;

internal sealed class AutoDisposingDynamicQueryExecutor : IDynamicQueryExecutor
{
    private readonly IDynamicQueryExecutor _innerExecutor;
    private readonly IDisposable _resource;

    public AutoDisposingDynamicQueryExecutor(IDynamicQueryExecutor innerExecutor, IDisposable resource)
    {
        _innerExecutor = innerExecutor;
        _resource = resource;
    }

    public List<JsonDocument> ExecuteQuery(DynamicQueryBuilder query)
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

    public int ExecuteCount(DynamicQueryBuilder query)
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

    public async Task<List<JsonDocument>> ExecuteQueryAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
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

    public async Task<int> ExecuteCountAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
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

    public bool ExecuteAny(DynamicQueryBuilder query)
    {
        try
        {
            return _innerExecutor.ExecuteAny(query);
        }
        finally
        {
            _resource.Dispose();
        }
    }

    public async Task<bool> ExecuteAnyAsync(DynamicQueryBuilder query, CancellationToken cancellationToken)
    {
        try
        {
            return await _innerExecutor.ExecuteAnyAsync(query, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _resource.Dispose();
        }
    }

    public QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters)
    {
        return _innerExecutor.GetQueryExplanation(filters);
    }
}
