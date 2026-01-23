using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Query;

/// <summary>
/// Executes queries against a collection of documents.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public interface IQueryExecutor<T>
{
    /// <summary>
    /// Executes a query and returns matching documents.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <returns>List of matching documents.</returns>
    List<T> ExecuteQuery(QueryBuilder<T> query);

    /// <summary>
    /// Executes a query and returns the count of matching documents.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <returns>Number of matching documents.</returns>
    int ExecuteCount(QueryBuilder<T> query);

    /// <summary>
    /// Executes a query asynchronously and returns matching documents.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of matching documents.</returns>
    Task<List<T>> ExecuteQueryAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a count query asynchronously.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of matching documents.</returns>
    Task<int> ExecuteCountAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a query and returns true if any document matches.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <returns>True if at least one document matches the query.</returns>
    bool ExecuteAny(QueryBuilder<T> query);

    /// <summary>
    /// Executes a query asynchronously and returns true if any document matches.
    /// </summary>
    /// <param name="query">The query to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if at least one document matches the query.</returns>
    Task<bool> ExecuteAnyAsync(QueryBuilder<T> query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the query execution plan without executing the query.
    /// </summary>
    /// <param name="filters">The filters to analyze.</param>
    /// <returns>Explanation of how the query would be executed.</returns>
    QueryExplanation GetQueryExplanation(IReadOnlyList<IFieldFilter> filters);
}
