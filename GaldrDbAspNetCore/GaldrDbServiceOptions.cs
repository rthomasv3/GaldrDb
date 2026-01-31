using GaldrDbEngine;

namespace GaldrDbAspNetCore;

/// <summary>
/// Configuration options for the GaldrDb dependency injection service.
/// </summary>
public sealed class GaldrDbServiceOptions
{
    /// <summary>
    /// Gets or sets the path to the database file.
    /// </summary>
    public string FilePath { get; set; }

    /// <summary>
    /// Gets or sets how the database should be opened. Default is OpenOrCreate.
    /// </summary>
    public GaldrDbOpenMode OpenMode { get; set; } = GaldrDbOpenMode.OpenOrCreate;

    /// <summary>
    /// Gets or sets the database configuration options. If null, default options are used.
    /// </summary>
    public GaldrDbOptions DatabaseOptions { get; set; }
}
