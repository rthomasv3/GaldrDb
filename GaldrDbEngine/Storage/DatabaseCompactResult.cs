namespace GaldrDbEngine.Storage;

public sealed class DatabaseCompactResult
{
    public int CollectionsCompacted { get; }
    public int DocumentsCopied { get; }
    public long SourceFileSize { get; }
    public long TargetFileSize { get; }
    public long BytesSaved { get; }

    public DatabaseCompactResult(
        int collectionsCompacted,
        int documentsCopied,
        long sourceFileSize,
        long targetFileSize)
    {
        CollectionsCompacted = collectionsCompacted;
        DocumentsCopied = documentsCopied;
        SourceFileSize = sourceFileSize;
        TargetFileSize = targetFileSize;
        BytesSaved = sourceFileSize - targetFileSize;
    }
}
