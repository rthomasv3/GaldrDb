namespace GaldrDbSourceGenerators
{
    internal sealed class NestedPathResolution
    {
        public PropertyInfo Property { get; }
        public GaldrFieldTypeInfo FieldType { get; }

        public NestedPathResolution(PropertyInfo property, GaldrFieldTypeInfo fieldType)
        {
            Property = property;
            FieldType = fieldType;
        }
    }
}
