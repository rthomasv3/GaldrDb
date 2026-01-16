using System;
using System.IO;
using GaldrDbEngine.IO;

namespace GaldrDbEngine;

public class GaldrDbOptions
{
    public int PageSize { get; set; } = 8192; // must be power of 2
    public bool UseWal { get; set; } = true;
    public bool UseMmap { get; set; } = true; // auto-detect/fallback if unsupported
    
    // public int CompressionThreshold { get; set; } = int.MaxValue;

    // WAL checkpoint options
    public int WalCheckpointThreshold { get; set; } = 1000; // checkpoint after N frames
    public bool AutoCheckpoint { get; set; } = true;

    // Garbage collection options
    public int GarbageCollectionThreshold { get; set; } = 250; // run GC after N commits
    public bool AutoGarbageCollection { get; set; } = true;

    // Pool warmup options
    public bool WarmupOnOpen { get; set; } = true;
    public int JsonWriterBufferSize { get; set; } = 4096; // initial buffer size for JSON serialization
    public int JsonWriterPoolWarmupCount { get; set; } = 4; // number of writers to pre-create

    // Internal: for simulation testing only (null in production)
    internal IPageIO CustomPageIO { get; set; }
    internal Stream CustomWalStream { get; set; }
    internal Func<uint> CustomWalSaltGenerator { get; set; }
}