using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query;

/// <summary>
/// Executes dynamic queries and returns results.
/// </summary>
internal interface IDynamicQueryExecutor
{
    List<JsonDocument> ExecuteQuery(DynamicQueryBuilder query);
    int ExecuteCount(DynamicQueryBuilder query);
    Task<List<JsonDocument>> ExecuteQueryAsync(DynamicQueryBuilder query, CancellationToken cancellationToken);
    Task<int> ExecuteCountAsync(DynamicQueryBuilder query, CancellationToken cancellationToken);
    QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters);
}
