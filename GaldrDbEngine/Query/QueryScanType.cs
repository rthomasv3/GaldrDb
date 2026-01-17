namespace GaldrDbEngine.Query;

/// <summary>
/// The type of scan used to execute a query.
/// </summary>
public enum QueryScanType
{
    /// <summary>Scans all documents in the collection.</summary>
    FullScan,
    /// <summary>Uses the primary key B+ tree for a range scan.</summary>
    PrimaryKeyRange,
    /// <summary>Uses a secondary index for the scan.</summary>
    SecondaryIndex
}
