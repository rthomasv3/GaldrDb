using System;

namespace GaldrDbEngine.Query;

public sealed class OrderByClause<TDocument>
{
    public string FieldName { get; }
    public bool Descending { get; }
    public Comparison<TDocument> Comparer { get; }

    public OrderByClause(string fieldName, bool descending, Comparison<TDocument> comparer)
    {
        FieldName = fieldName;
        Descending = descending;
        Comparer = comparer;
    }
}
