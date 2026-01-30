using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class IndexedFilterResult
{
    public IFieldFilter Filter { get; }
    public IndexDefinition IndexDefinition { get; }
    public int FilterIndex { get; }

    /// <summary>
    /// For compound indexes: list of matched filters in index field order.
    /// Null for single-field indexes.
    /// </summary>
    public IReadOnlyList<IFieldFilter> MatchedFilters { get; }

    /// <summary>
    /// For compound indexes: list of filter indices that are used by this index.
    /// Null for single-field indexes.
    /// </summary>
    public IReadOnlyList<int> MatchedFilterIndices { get; }

    /// <summary>
    /// True if this is a compound index result.
    /// </summary>
    public bool IsCompoundIndex => MatchedFilters != null;

    /// <summary>
    /// For compound indexes: number of equality fields matched.
    /// </summary>
    public int EqualityFieldCount { get; }

    /// <summary>
    /// For compound indexes: true if there's a range filter on the last matched field.
    /// </summary>
    public bool HasRangeField { get; }

    public IndexedFilterResult(IFieldFilter filter, IndexDefinition indexDefinition, int filterIndex)
    {
        Filter = filter;
        IndexDefinition = indexDefinition;
        FilterIndex = filterIndex;
        MatchedFilters = null;
        MatchedFilterIndices = null;
        EqualityFieldCount = 0;
        HasRangeField = false;
    }

    public IndexedFilterResult(
        IFieldFilter leadingFilter,
        IndexDefinition indexDefinition,
        int leadingFilterIndex,
        IReadOnlyList<IFieldFilter> matchedFilters,
        IReadOnlyList<int> matchedFilterIndices,
        int equalityFieldCount,
        bool hasRangeField)
    {
        Filter = leadingFilter;
        IndexDefinition = indexDefinition;
        FilterIndex = leadingFilterIndex;
        MatchedFilters = matchedFilters;
        MatchedFilterIndices = matchedFilterIndices;
        EqualityFieldCount = equalityFieldCount;
        HasRangeField = hasRangeField;
    }
}
