using System;

namespace GaldrDbEngine.Attributes;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class GaldrIndexAttribute : Attribute
{
    /// <summary>
    /// If true, this index enforces uniqueness.
    /// </summary>
    public bool Unique { get; set; } = false;
    
    public GaldrIndexAttribute() { }
}