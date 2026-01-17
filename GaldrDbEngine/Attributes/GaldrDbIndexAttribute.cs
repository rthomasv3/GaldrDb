using System;

namespace GaldrDbEngine.Attributes;

/// <summary>
/// Marks a property for secondary index creation.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class GaldrDbIndexAttribute : Attribute
{
    /// <summary>
    /// If true, this index enforces uniqueness.
    /// </summary>
    public bool Unique { get; set; } = false;

    /// <summary>
    /// Creates a new index attribute.
    /// </summary>
    public GaldrDbIndexAttribute() { }
}