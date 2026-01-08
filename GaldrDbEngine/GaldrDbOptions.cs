namespace GaldrDbEngine;

public class GaldrDbOptions
{
    public int PageSize { get; set; } = 8192; // must be power of 2
    public bool UseWal { get; set; } = true;
    public bool UseMmap { get; set; } = true; // auto-detect/fallback if unsupported
    public int CompressionThreshold { get; set; } = int.MaxValue;

    // WAL checkpoint options
    public int WalCheckpointThreshold { get; set; } = 1000; // checkpoint after N frames
    public bool AutoCheckpoint { get; set; } = true;

    // Garbage collection options
    public int GarbageCollectionThreshold { get; set; } = 100; // run GC after N commits
    public bool AutoGarbageCollection { get; set; } = true;
}