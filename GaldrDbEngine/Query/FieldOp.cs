namespace GaldrDbEngine.Query;

/// <summary>
/// Comparison operators for query filters.
/// </summary>
public enum FieldOp
{
    /// <summary>Field equals value.</summary>
    Equals,
    /// <summary>Field does not equal value.</summary>
    NotEquals,
    /// <summary>Field is greater than value.</summary>
    GreaterThan,
    /// <summary>Field is greater than or equal to value.</summary>
    GreaterThanOrEqual,
    /// <summary>Field is less than value.</summary>
    LessThan,
    /// <summary>Field is less than or equal to value.</summary>
    LessThanOrEqual,
    /// <summary>String field starts with value.</summary>
    StartsWith,
    /// <summary>String field ends with value.</summary>
    EndsWith,
    /// <summary>String field contains value.</summary>
    Contains,
    /// <summary>Field is between two values (inclusive).</summary>
    Between,
    /// <summary>Field is one of the specified values.</summary>
    In,
    /// <summary>Field is not one of the specified values.</summary>
    NotIn
}
