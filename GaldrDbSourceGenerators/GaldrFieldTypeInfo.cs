namespace GaldrDbSourceGenerators
{
    internal sealed class GaldrFieldTypeInfo
    {
        public string FieldTypeEnum { get; }
        public string WriteMethod { get; }
        public bool IsNullable { get; }
        public bool IsEnum { get; }
        public string EnumCastType { get; }

        public GaldrFieldTypeInfo(string fieldTypeEnum, string writeMethod)
        {
            FieldTypeEnum = fieldTypeEnum;
            WriteMethod = writeMethod;
            IsNullable = false;
            IsEnum = false;
            EnumCastType = null;
        }

        public GaldrFieldTypeInfo(string fieldTypeEnum, string writeMethod, bool isNullable)
        {
            FieldTypeEnum = fieldTypeEnum;
            WriteMethod = writeMethod;
            IsNullable = isNullable;
            IsEnum = false;
            EnumCastType = null;
        }

        public GaldrFieldTypeInfo(string fieldTypeEnum, string writeMethod, bool isNullable, bool isEnum, string enumCastType)
        {
            FieldTypeEnum = fieldTypeEnum;
            WriteMethod = writeMethod;
            IsNullable = isNullable;
            IsEnum = isEnum;
            EnumCastType = enumCastType;
        }
    }
}
