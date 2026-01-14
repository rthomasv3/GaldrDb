using System;

namespace GaldrDbEngine.Query;

public static class QueryBuilderExtensions
{
    public static QueryBuilder<T> Where<T, TSource, TField>(
        this QueryBuilder<T> builder,
        GaldrField<TSource, TField> field,
        FieldOp op,
        TField value)
        where T : IProjectionOf<TSource>
    {
        builder.AddFilter(new FieldFilter<TSource, TField>(field, op, value));
        return builder;
    }

    public static QueryBuilder<T> WhereBetween<T, TSource, TField>(
        this QueryBuilder<T> builder,
        GaldrField<TSource, TField> field,
        TField minValue,
        TField maxValue)
        where T : IProjectionOf<TSource>
        where TField : IComparable<TField>
    {
        builder.AddFilter(new BetweenFilter<TSource, TField>(field, minValue, maxValue));
        return builder;
    }

    public static QueryBuilder<T> WhereIn<T, TSource, TField>(
        this QueryBuilder<T> builder,
        GaldrField<TSource, TField> field,
        params TField[] values)
        where T : IProjectionOf<TSource>
    {
        builder.AddFilter(new InFilter<TSource, TField>(field, values));
        return builder;
    }

    public static QueryBuilder<T> WhereNotIn<T, TSource, TField>(
        this QueryBuilder<T> builder,
        GaldrField<TSource, TField> field,
        params TField[] values)
        where T : IProjectionOf<TSource>
    {
        builder.AddFilter(new NotInFilter<TSource, TField>(field, values));
        return builder;
    }
}
