using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Planning;

internal sealed class SecondaryIndexSpec
{
    public IndexDefinition IndexDefinition { get; }
    public IFieldFilter IndexFilter { get; }
    public SecondaryIndexOperation Operation { get; }
    public ScanDirection Direction { get; }

    /// <summary>
    /// For compound index scans: the list of matched filters in index field order.
    /// Null for single-field index scans.
    /// </summary>
    public IReadOnlyList<IFieldFilter> MatchedFilters { get; }

    /// <summary>
    /// For compound index scans: pre-computed start key bytes for range scan.
    /// Null for single-field index scans.
    /// </summary>
    public byte[] CompoundStartKey { get; }

    /// <summary>
    /// For compound index scans: pre-computed end key bytes for range scan.
    /// Null for single-field index scans.
    /// </summary>
    public byte[] CompoundEndKey { get; }

    /// <summary>
    /// For compound index prefix-range scans: the equality prefix key.
    /// Null for other operations.
    /// </summary>
    public byte[] CompoundPrefixKey { get; }

    /// <summary>
    /// True if this is a compound index scan.
    /// </summary>
    public bool IsCompoundScan => MatchedFilters != null;

    public SecondaryIndexSpec(IndexDefinition indexDefinition, IFieldFilter indexFilter, SecondaryIndexOperation operation, ScanDirection direction)
    {
        IndexDefinition = indexDefinition;
        IndexFilter = indexFilter;
        Operation = operation;
        Direction = direction;
        MatchedFilters = null;
        CompoundStartKey = null;
        CompoundEndKey = null;
        CompoundPrefixKey = null;
    }

    public SecondaryIndexSpec(
        IndexDefinition indexDefinition,
        IFieldFilter leadingFilter,
        SecondaryIndexOperation operation,
        ScanDirection direction,
        IReadOnlyList<IFieldFilter> matchedFilters,
        byte[] compoundStartKey,
        byte[] compoundEndKey,
        byte[] compoundPrefixKey = null)
    {
        IndexDefinition = indexDefinition;
        IndexFilter = leadingFilter;
        Operation = operation;
        Direction = direction;
        MatchedFilters = matchedFilters;
        CompoundStartKey = compoundStartKey;
        CompoundEndKey = compoundEndKey;
        CompoundPrefixKey = compoundPrefixKey;
    }
}
