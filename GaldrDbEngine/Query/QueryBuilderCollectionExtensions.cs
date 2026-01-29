using System;

namespace GaldrDbEngine.Query;

/// <summary>
/// Extension methods for querying collection properties on documents.
/// </summary>
public static class QueryBuilderCollectionExtensions
{
    /// <summary>
    /// Adds a filter that matches documents where any element in a collection satisfies the condition.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <typeparam name="TElement">The collection element type.</typeparam>
    /// <typeparam name="TField">The field type within elements.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="op">The comparison operation.</param>
    /// <param name="value">The value to compare against.</param>
    /// <returns>The query builder for chaining.</returns>
    public static QueryBuilder<T> WhereAny<T, TElement, TField>(
        this QueryBuilder<T> builder,
        GaldrCollectionField<T, TElement, TField> field,
        FieldOp op,
        TField value)
    {
        builder.AddFilter(new CollectionFieldFilter<T, TElement, TField>(field, op, value));
        return builder;
    }

    /// <summary>
    /// Adds a between filter that matches documents where any element in a collection
    /// has a field value within the specified range (inclusive).
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <typeparam name="TElement">The collection element type.</typeparam>
    /// <typeparam name="TField">The field type within elements.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="minValue">The minimum value (inclusive).</param>
    /// <param name="maxValue">The maximum value (inclusive).</param>
    /// <returns>The query builder for chaining.</returns>
    public static QueryBuilder<T> WhereAnyBetween<T, TElement, TField>(
        this QueryBuilder<T> builder,
        GaldrCollectionField<T, TElement, TField> field,
        TField minValue,
        TField maxValue)
        where TField : IComparable<TField>
    {
        builder.AddFilter(new CollectionBetweenFilter<T, TElement, TField>(field, minValue, maxValue));
        return builder;
    }

    /// <summary>
    /// Adds an IN filter that matches documents where any element in a collection
    /// has a field value matching one of the specified values.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <typeparam name="TElement">The collection element type.</typeparam>
    /// <typeparam name="TField">The field type within elements.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="values">The values to match against.</param>
    /// <returns>The query builder for chaining.</returns>
    public static QueryBuilder<T> WhereAnyIn<T, TElement, TField>(
        this QueryBuilder<T> builder,
        GaldrCollectionField<T, TElement, TField> field,
        params TField[] values)
    {
        builder.AddFilter(new CollectionInFilter<T, TElement, TField>(field, values));
        return builder;
    }

    /// <summary>
    /// Adds a NOT IN filter that matches documents where any element in a collection
    /// has a field value not in the specified values.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <typeparam name="TElement">The collection element type.</typeparam>
    /// <typeparam name="TField">The field type within elements.</typeparam>
    /// <param name="builder">The query builder.</param>
    /// <param name="field">The collection field to filter on.</param>
    /// <param name="values">The values to exclude.</param>
    /// <returns>The query builder for chaining.</returns>
    public static QueryBuilder<T> WhereAnyNotIn<T, TElement, TField>(
        this QueryBuilder<T> builder,
        GaldrCollectionField<T, TElement, TField> field,
        params TField[] values)
    {
        builder.AddFilter(new CollectionNotInFilter<T, TElement, TField>(field, values));
        return builder;
    }
}
