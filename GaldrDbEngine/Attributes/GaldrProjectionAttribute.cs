using System;

namespace GaldrDbEngine.Attributes;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public class GaldrProjectionAttribute : Attribute
{
    public string CollectionName { get; }
    
    public GaldrProjectionAttribute(string collectionName)
    {
        CollectionName = collectionName;
    }
}