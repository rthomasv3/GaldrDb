using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

public interface IQueryExecutor<T>
{
    List<T> ExecuteQuery(QueryBuilder<T> query);
    int ExecuteCount(QueryBuilder<T> query);
    Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default);
    Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default);
    QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters);
}
