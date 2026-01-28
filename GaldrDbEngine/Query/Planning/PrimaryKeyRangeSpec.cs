namespace GaldrDbEngine.Query.Planning;

internal sealed class PrimaryKeyRangeSpec
{
    public int StartDocId { get; }
    public int EndDocId { get; }
    public bool IncludeStart { get; }
    public bool IncludeEnd { get; }

    public PrimaryKeyRangeSpec(int startDocId, int endDocId, bool includeStart, bool includeEnd)
    {
        StartDocId = startDocId;
        EndDocId = endDocId;
        IncludeStart = includeStart;
        IncludeEnd = includeEnd;
    }
}
