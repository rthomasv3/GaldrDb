namespace GaldrDbAspNetCore;

/// <summary>
/// Specifies how the database should be opened.
/// </summary>
public enum GaldrDbOpenMode
{
    /// <summary>
    /// Open an existing database. Throws if the file does not exist.
    /// </summary>
    Open,

    /// <summary>
    /// Create a new database. Throws if the file already exists.
    /// </summary>
    Create,

    /// <summary>
    /// Open an existing database or create a new one if it does not exist.
    /// </summary>
    OpenOrCreate
}
