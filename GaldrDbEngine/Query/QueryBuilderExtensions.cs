using System;

namespace GaldrDbEngine.Query;

/// <summary>
/// Extension methods for querying projection types using source document fields.
/// </summary>
public static class QueryBuilderExtensions
{
    /// <summary>
    /// Adds a filter using a source document field.
    /// </summary>
    /// <typeparam name="T">The projection type.</typeparam>
    /// <typeparam name="TSource">The source document type.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The source field to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The query builder for chaining.</returns>
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

    /// <summary>
    /// Adds a between filter using a source document field.
    /// </summary>
    /// <typeparam name="T">The projection type.</typeparam>
    /// <typeparam name="TSource">The source document type.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The source field to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    /// <returns>The query builder for chaining.</returns>
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

    /// <summary>
    /// Adds an IN filter using a source document field.
    /// </summary>
    /// <typeparam name="T">The projection type.</typeparam>
    /// <typeparam name="TSource">The source document type.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The source field to filter on.</param>
    /// <param name="values">The values to match against.</param>
    /// <returns>The query builder for chaining.</returns>
    public static QueryBuilder<T> WhereIn<T, TSource, TField>(
        this QueryBuilder<T> builder,
        GaldrField<TSource, TField> field,
        params TField[] values)
        where T : IProjectionOf<TSource>
    {
        builder.AddFilter(new InFilter<TSource, TField>(field, values));
        return builder;
    }

    /// <summary>
    /// Adds a NOT IN filter using a source document field.
    /// </summary>
    /// <typeparam name="T">The projection type.</typeparam>
    /// <typeparam name="TSource">The source document type.</typeparam>
    /// <typeparam name="TField">The field type.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The source field to filter on.</param>
    /// <param name="values">The values to exclude.</param>
    /// <returns>The query builder for chaining.</returns>
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
