using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query;

/// <summary>
/// Builds dynamic queries for filtering and retrieving documents using runtime field names.
/// </summary>
public sealed class DynamicQueryBuilder
{
    private readonly string _collectionName;
    private readonly CollectionEntry _collection;
    private readonly IDynamicQueryExecutor _executor;
    private readonly List<IFieldFilter> _filters;
    private readonly List<DynamicOrderByClause> _orderByClauses;
    private int? _limit;
    private int? _skip;

    internal DynamicQueryBuilder(string collectionName, CollectionEntry collection, IDynamicQueryExecutor executor)
    {
        _collectionName = collectionName;
        _collection = collection;
        _executor = executor;
        _filters = new List<IFieldFilter>();
        _orderByClauses = new List<DynamicOrderByClause>();
    }

    /// <summary>
    /// Adds a filter condition to the query.
    /// </summary>
    /// <param name="fieldName">The field name to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder Where(string fieldName, FieldOp op, object value)
    {
        IndexDefinition indexDef = _collection?.FindIndex(fieldName);
        bool isIndexed = indexDef != null;
        GaldrFieldType fieldType = indexDef?.FieldType ?? InferTypeFromValue(value);

        _filters.Add(new DynamicFieldFilter(fieldName, fieldType, op, value, isIndexed));
        return this;
    }

    /// <summary>
    /// Adds a between filter to match values within a range (inclusive on both ends).
    /// </summary>
    /// <param name="fieldName">The field name to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder WhereBetween(string fieldName, object minValue, object maxValue)
    {
        IndexDefinition indexDef = _collection?.FindIndex(fieldName);
        bool isIndexed = indexDef != null;
        GaldrFieldType fieldType = indexDef?.FieldType ?? InferTypeFromValue(minValue);

        _filters.Add(new DynamicBetweenFilter(fieldName, fieldType, minValue, maxValue, isIndexed));
        return this;
    }

    /// <summary>
    /// Adds an IN filter to match any of the specified values.
    /// </summary>
    /// <param name="fieldName">The field name to filter on.</param>
    /// <param name="values">The values to match against.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder WhereIn(string fieldName, params object[] values)
    {
        IndexDefinition indexDef = _collection?.FindIndex(fieldName);
        bool isIndexed = indexDef != null;
        GaldrFieldType fieldType = indexDef?.FieldType ?? (values.Length > 0 ? InferTypeFromValue(values[0]) : GaldrFieldType.String);

        _filters.Add(new DynamicInFilter(fieldName, fieldType, values, isIndexed));
        return this;
    }

    /// <summary>
    /// Adds a NOT IN filter to exclude the specified values.
    /// </summary>
    /// <param name="fieldName">The field name to filter on.</param>
    /// <param name="values">The values to exclude.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder WhereNotIn(string fieldName, params object[] values)
    {
        IndexDefinition indexDef = _collection?.FindIndex(fieldName);
        GaldrFieldType fieldType = indexDef?.FieldType ?? (values.Length > 0 ? InferTypeFromValue(values[0]) : GaldrFieldType.String);

        _filters.Add(new DynamicNotInFilter(fieldName, fieldType, values));
        return this;
    }

    /// <summary>
    /// Limits the number of results returned.
    /// </summary>
    /// <param name="count">Maximum number of documents to return.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder Limit(int count)
    {
        _limit = count;
        return this;
    }

    /// <summary>
    /// Skips a number of results before returning.
    /// </summary>
    /// <param name="count">Number of documents to skip.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder Skip(int count)
    {
        _skip = count;
        return this;
    }

    /// <summary>
    /// Sorts results by a field in ascending order.
    /// </summary>
    /// <param name="fieldName">The field name to sort by.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder OrderBy(string fieldName)
    {
        _orderByClauses.Add(new DynamicOrderByClause(fieldName, false));
        return this;
    }

    /// <summary>
    /// Sorts results by a field in descending order.
    /// </summary>
    /// <param name="fieldName">The field name to sort by.</param>
    /// <returns>This query builder for chaining.</returns>
    public DynamicQueryBuilder OrderByDescending(string fieldName)
    {
        _orderByClauses.Add(new DynamicOrderByClause(fieldName, true));
        return this;
    }

    /// <summary>
    /// Executes the query and returns all matching documents.
    /// </summary>
    /// <returns>List of matching documents.</returns>
    public List<JsonDocument> ToList()
    {
        return _executor.ExecuteQuery(this);
    }

    /// <summary>
    /// Executes the query and returns the first matching document, or null if none.
    /// </summary>
    /// <returns>The first matching document, or null.</returns>
    public JsonDocument FirstOrDefault()
    {
        int? originalLimit = _limit;
        _limit = 1;

        List<JsonDocument> results = ToList();

        _limit = originalLimit;

        JsonDocument result = null;
        if (results.Count > 0)
        {
            result = results[0];
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
    public Task<List<JsonDocument>> ToListAsync(CancellationToken cancellationToken = default)
    {
        return _executor.ExecuteQueryAsync(this, cancellationToken);
    }

    /// <summary>
    /// Executes the query asynchronously and returns the first matching document.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The first matching document, or null.</returns>
    public async Task<JsonDocument> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        int? originalLimit = _limit;
        _limit = 1;

        List<JsonDocument> results = await _executor.ExecuteQueryAsync(this, cancellationToken).ConfigureAwait(false);

        _limit = originalLimit;

        JsonDocument result = null;
        if (results.Count > 0)
        {
            result = results[0];
        }

        return result;
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

    internal string CollectionName
    {
        get { return _collectionName; }
    }

    internal IReadOnlyList<IFieldFilter> Filters
    {
        get { return _filters; }
    }

    internal IReadOnlyList<DynamicOrderByClause> OrderByClauses
    {
        get { return _orderByClauses; }
    }

    internal int? LimitValue
    {
        get { return _limit; }
    }

    internal int? SkipValue
    {
        get { return _skip; }
    }

    internal IDynamicQueryExecutor GetExecutor()
    {
        return _executor;
    }

    private static GaldrFieldType InferTypeFromValue(object value)
    {
        if (value == null)
        {
            return GaldrFieldType.String;
        }

        return value switch
        {
            int => GaldrFieldType.Int32,
            long => GaldrFieldType.Int64,
            string => GaldrFieldType.String,
            bool => GaldrFieldType.Boolean,
            System.DateTime => GaldrFieldType.DateTime,
            System.DateTimeOffset => GaldrFieldType.DateTimeOffset,
            System.Guid => GaldrFieldType.Guid,
            double => GaldrFieldType.Double,
            decimal => GaldrFieldType.Decimal,
            byte => GaldrFieldType.Byte,
            sbyte => GaldrFieldType.SByte,
            short => GaldrFieldType.Int16,
            ushort => GaldrFieldType.UInt16,
            uint => GaldrFieldType.UInt32,
            ulong => GaldrFieldType.UInt64,
            float => GaldrFieldType.Single,
            char => GaldrFieldType.Char,
            System.TimeSpan => GaldrFieldType.TimeSpan,
            System.DateOnly => GaldrFieldType.DateOnly,
            System.TimeOnly => GaldrFieldType.TimeOnly,
            _ => GaldrFieldType.String
        };
    }
}
