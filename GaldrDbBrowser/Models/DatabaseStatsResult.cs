namespace GaldrDbBrowser.Models;

public class DatabaseStatsResult
{
    public bool Success { get; set; }
    public string Error { get; set; }
    public string FilePath { get; set; }
    public long FileSizeBytes { get; set; }
    public int CollectionCount { get; set; }
    public int PageSize { get; set; }
}
