namespace GaldrDbEngine.Query;

/// <summary>
/// Supported field types for indexing and filtering.
/// </summary>
public enum GaldrFieldType
{
    /// <summary>32-bit integer.</summary>
    Int32,
    /// <summary>64-bit integer.</summary>
    Int64,
    /// <summary>String.</summary>
    String,
    /// <summary>Boolean.</summary>
    Boolean,
    /// <summary>DateTime.</summary>
    DateTime,
    /// <summary>DateTimeOffset.</summary>
    DateTimeOffset,
    /// <summary>GUID.</summary>
    Guid,
    /// <summary>Double-precision floating point.</summary>
    Double,
    /// <summary>Decimal.</summary>
    Decimal,
    /// <summary>Complex/nested type (not indexable).</summary>
    Complex
}
