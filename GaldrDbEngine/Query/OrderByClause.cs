using System;

namespace GaldrDbEngine.Query;

/// <summary>
/// Represents an ordering clause for query results.
/// </summary>
/// <typeparam name="TDocument">The document type.</typeparam>
public sealed class OrderByClause<TDocument>
{
    /// <summary>The name of the field to order by.</summary>
    public string FieldName { get; }

    /// <summary>Whether to sort in descending order.</summary>
    public bool Descending { get; }

    /// <summary>The comparison function for sorting.</summary>
    public Comparison<TDocument> Comparer { get; }

    /// <summary>
    /// Creates a new OrderByClause.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="descending">Whether to sort descending.</param>
    /// <param name="comparer">The comparison function.</param>
    public OrderByClause(string fieldName, bool descending, Comparison<TDocument> comparer)
    {
        FieldName = fieldName;
        Descending = descending;
        Comparer = comparer;
    }
}
