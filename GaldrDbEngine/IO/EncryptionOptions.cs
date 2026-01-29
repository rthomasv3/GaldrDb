namespace GaldrDbEngine.IO;

/// <summary>
/// Configuration options for database encryption.
/// </summary>
public sealed class EncryptionOptions
{
    /// <summary>
    /// The password used for encryption key derivation.
    /// </summary>
    public string Password { get; set; }

    /// <summary>
    /// Number of PBKDF2 iterations for key derivation.
    /// Higher values provide better security but slower database open.
    /// Default is 500,000 iterations.
    /// </summary>
    public int KdfIterations { get; set; } = 500000;
}
