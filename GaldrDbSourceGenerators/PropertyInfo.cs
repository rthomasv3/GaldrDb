using System.Collections.Immutable;

namespace GaldrDbSourceGenerators
{
    internal sealed class PropertyInfo
    {
        public string Name { get; }
        public string TypeName { get; }
        public GaldrFieldTypeInfo FieldType { get; }
        public bool IsIndexed { get; }
        public bool IsUniqueIndex { get; }
        public bool IsNestedObject { get; }
        public bool IsCollection { get; }
        public string CollectionElementTypeName { get; }
        public ImmutableArray<PropertyInfo> NestedProperties { get; }

        public PropertyInfo(string name, string typeName, GaldrFieldTypeInfo fieldType, bool isIndexed, bool isUniqueIndex)
        {
            Name = name;
            TypeName = typeName;
            FieldType = fieldType;
            IsIndexed = isIndexed;
            IsUniqueIndex = isUniqueIndex;
            IsNestedObject = false;
            IsCollection = false;
            CollectionElementTypeName = null;
            NestedProperties = ImmutableArray<PropertyInfo>.Empty;
        }

        public PropertyInfo(
            string name,
            string typeName,
            GaldrFieldTypeInfo fieldType,
            bool isIndexed,
            bool isUniqueIndex,
            bool isNestedObject,
            bool isCollection,
            string collectionElementTypeName,
            ImmutableArray<PropertyInfo> nestedProperties)
        {
            Name = name;
            TypeName = typeName;
            FieldType = fieldType;
            IsIndexed = isIndexed;
            IsUniqueIndex = isUniqueIndex;
            IsNestedObject = isNestedObject;
            IsCollection = isCollection;
            CollectionElementTypeName = collectionElementTypeName;
            NestedProperties = nestedProperties;
        }
    }
}
