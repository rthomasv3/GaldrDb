using System;

namespace GaldrDbEngine.Attributes;

/// <summary>
/// Declares a compound index on multiple fields of a document type.
/// Field order matters - the index can efficiently serve queries that filter on
/// the leftmost prefix of fields (e.g., for index on (A, B, C): A, A+B, or A+B+C).
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class GaldrDbCompoundIndexAttribute : Attribute
{
    /// <summary>
    /// The field names that make up this compound index, in order.
    /// </summary>
    public string[] FieldNames { get; }

    /// <summary>
    /// If true, this compound index enforces uniqueness on the combination of field values.
    /// Two documents with the same values for all fields in the index would violate the constraint.
    /// Null values do not violate uniqueness (following standard database behavior).
    /// </summary>
    public bool Unique { get; set; } = false;

    /// <summary>
    /// Creates a compound index on the specified fields.
    /// </summary>
    /// <param name="fieldNames">The field names in index order (2-8 fields required).</param>
    public GaldrDbCompoundIndexAttribute(params string[] fieldNames)
    {
        FieldNames = fieldNames;
    }
}
