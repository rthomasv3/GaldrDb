namespace GaldrDbSourceGenerators
{
    internal sealed class NestedIndexedProperty
    {
        public string Path { get; }
        public GaldrFieldTypeInfo FieldType { get; }
        public bool IsUnique { get; }

        public NestedIndexedProperty(string path, GaldrFieldTypeInfo fieldType, bool isUnique)
        {
            Path = path;
            FieldType = fieldType;
            IsUnique = isUnique;
        }
    }
}
