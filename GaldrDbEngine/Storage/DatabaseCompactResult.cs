namespace GaldrDbEngine.Storage;

/// <summary>
/// Contains statistics from a database compaction operation.
/// </summary>
public sealed class DatabaseCompactResult
{
    /// <summary>Number of collections that were compacted.</summary>
    public int CollectionsCompacted { get; }

    /// <summary>Number of documents that were copied to the new file.</summary>
    public int DocumentsCopied { get; }

    /// <summary>Size of the source database file in bytes.</summary>
    public long SourceFileSize { get; }

    /// <summary>Size of the compacted database file in bytes.</summary>
    public long TargetFileSize { get; }

    /// <summary>Number of bytes saved by compaction.</summary>
    public long BytesSaved { get; }

    /// <summary>
    /// Creates a new database compact result.
    /// </summary>
    /// <param name="collectionsCompacted">Number of collections compacted.</param>
    /// <param name="documentsCopied">Number of documents copied.</param>
    /// <param name="sourceFileSize">Source file size in bytes.</param>
    /// <param name="targetFileSize">Target file size in bytes.</param>
    internal DatabaseCompactResult(
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
