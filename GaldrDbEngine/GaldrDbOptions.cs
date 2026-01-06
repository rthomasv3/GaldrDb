namespace GaldrDbEngine;

public class GaldrDbOptions
{
    public int PageSize { get; set; } = 8192; // must be power of 2
    public bool UseWal { get; set; } = true;
    public bool UseMmap { get; set; } = true; // auto-detect/fallback if unsupported
    public int CompressionThreshold { get; set; } = int.MaxValue;
}