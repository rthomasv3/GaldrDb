namespace GaldrDbEngine.Transactions;

/// <summary>
/// Represents an indexed field value with its encoded key bytes.
/// </summary>
public class IndexFieldEntry
{
    /// <summary>The name of the indexed field.</summary>
    public string FieldName { get; }

    /// <summary>The encoded key bytes for the field value.</summary>
    public byte[] KeyBytes { get; }

    /// <summary>
    /// Creates a new index field entry.
    /// </summary>
    /// <param name="fieldName">The field name.</param>
    /// <param name="keyBytes">The encoded key bytes.</param>
    internal IndexFieldEntry(string fieldName, byte[] keyBytes)
    {
        FieldName = fieldName;
        KeyBytes = keyBytes;
    }
}
