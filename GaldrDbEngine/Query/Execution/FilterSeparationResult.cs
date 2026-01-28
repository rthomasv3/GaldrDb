using System.Collections.Generic;

namespace GaldrDbEngine.Query.Execution;

internal sealed class FilterSeparationResult
{
    public List<IFieldFilter> SourceFilters { get; }
    public List<IFieldFilter> ProjectionFilters { get; }

    public FilterSeparationResult(List<IFieldFilter> sourceFilters, List<IFieldFilter> projectionFilters)
    {
        SourceFilters = sourceFilters;
        ProjectionFilters = projectionFilters;
    }
}
