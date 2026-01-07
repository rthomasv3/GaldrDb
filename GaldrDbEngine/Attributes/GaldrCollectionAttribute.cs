using System;

namespace GaldrDbEngine.Attributes;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class GaldrCollectionAttribute : Attribute
{
    public string CollectionName { get; }
    
    public GaldrCollectionAttribute(string collectionName)
    {
        CollectionName = collectionName;
    }
}