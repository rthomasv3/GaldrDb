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
    /// <summary>Unsigned 8-bit integer.</summary>
    Byte,
    /// <summary>Signed 8-bit integer.</summary>
    SByte,
    /// <summary>Signed 16-bit integer.</summary>
    Int16,
    /// <summary>Unsigned 16-bit integer.</summary>
    UInt16,
    /// <summary>Unsigned 32-bit integer.</summary>
    UInt32,
    /// <summary>Unsigned 64-bit integer.</summary>
    UInt64,
    /// <summary>Single-precision floating point.</summary>
    Single,
    /// <summary>UTF-16 character.</summary>
    Char,
    /// <summary>Time interval.</summary>
    TimeSpan,
    /// <summary>Date without time.</summary>
    DateOnly,
    /// <summary>Time without date.</summary>
    TimeOnly,
    /// <summary>Complex/nested type (not indexable).</summary>
    Complex
}
