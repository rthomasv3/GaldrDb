using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class CompoundIndexMatch
{
    public IndexDefinition IndexDefinition { get; }
    public int Score { get; }
    public int EqualityFieldCount { get; }
    public bool HasRangeField { get; }
    public int LeadingFilterIndex { get; }
    public IReadOnlyList<IFieldFilter> MatchedFilters { get; }
    public IReadOnlyList<int> MatchedFilterIndices { get; }

    public CompoundIndexMatch(
        IndexDefinition indexDefinition,
        int score,
        int equalityFieldCount,
        bool hasRangeField,
        int leadingFilterIndex,
        IReadOnlyList<IFieldFilter> matchedFilters,
        IReadOnlyList<int> matchedFilterIndices)
    {
        IndexDefinition = indexDefinition;
        Score = score;
        EqualityFieldCount = equalityFieldCount;
        HasRangeField = hasRangeField;
        LeadingFilterIndex = leadingFilterIndex;
        MatchedFilters = matchedFilters;
        MatchedFilterIndices = matchedFilterIndices;
    }
}
