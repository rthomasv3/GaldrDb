using System;
using System.IO;
using GaldrDbEngine.IO;

namespace GaldrDbEngine;

/// <summary>
/// Configuration options for a GaldrDb database.
/// </summary>
public class GaldrDbOptions
{
    /// <summary>
    /// Page size in bytes. Must be a power of 2. Default is 8192.
    /// </summary>
    public int PageSize { get; set; } = 8192;

    /// <summary>
    /// Enable write-ahead logging for durability. Default is true.
    /// </summary>
    public bool UseWal { get; set; } = true;

    /// <summary>
    /// Use memory-mapped I/O when supported. Falls back to standard I/O if unavailable. Default is false.
    /// </summary>
    public bool UseMmap { get; set; } = false;

    // TODO: Implement compression support
    // /// <summary>
    // /// Document size threshold in bytes above which compression is applied. Default is int.MaxValue (disabled).
    // /// </summary>
    // public int CompressionThreshold { get; set; } = int.MaxValue;

    /// <summary>
    /// Number of WAL frames before automatic checkpoint. Default is 1000.
    /// </summary>
    public int WalCheckpointThreshold { get; set; } = 1000;

    /// <summary>
    /// Automatically checkpoint WAL when threshold is reached. Default is true.
    /// </summary>
    public bool AutoCheckpoint { get; set; } = true;

    /// <summary>
    /// Number of commits before automatic garbage collection. Default is 250.
    /// </summary>
    public int GarbageCollectionThreshold { get; set; } = 250;

    /// <summary>
    /// Automatically collect garbage when threshold is reached. Default is true.
    /// </summary>
    public bool AutoGarbageCollection { get; set; } = true;

    /// <summary>
    /// Pre-warm object pools when database is opened. Default is true.
    /// </summary>
    public bool WarmupOnOpen { get; set; } = true;

    /// <summary>
    /// Initial buffer size for JSON serialization. Default is 4096.
    /// </summary>
    public int JsonWriterBufferSize { get; set; } = 4096;

    /// <summary>
    /// Number of JSON writers to pre-create in the pool. Default is 4.
    /// </summary>
    public int JsonWriterPoolWarmupCount { get; set; } = 4;

    /// <summary>
    /// Number of pages to add when the database file needs to expand. Default is 256 pages.
    /// With the default 8KB page size, this equals 2MB per expansion.
    /// </summary>
    public int ExpansionPageCount { get; set; } = 256;

    /// <summary>
    /// Maximum number of pages to keep in the page cache. Default is 2000 pages.
    /// With the default 8KB page size, this equals 16MB of cache.
    /// Set to 0 to disable the page cache.
    /// </summary>
    public int PageCacheSize { get; set; } = 2000;

    // Internal: for simulation testing only (null in production)
    internal IPageIO CustomPageIO { get; set; }
    internal Stream CustomWalStream { get; set; }
    internal Func<uint> CustomWalSaltGenerator { get; set; }
}
