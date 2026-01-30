using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace GaldrDbSourceGenerators
{
    [Generator]
    public class GaldrDbGenerator : IIncrementalGenerator
    {
        // Diagnostic descriptors for Id property validation
        private static readonly DiagnosticDescriptor MissingIdProperty = new DiagnosticDescriptor(
            id: "GALDR001",
            title: "Missing Id property",
            messageFormat: "Type '{0}' marked with [GaldrJsonSerializable] must have a public 'int Id {{ get; set; }}' property",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor InvalidIdPropertyType = new DiagnosticDescriptor(
            id: "GALDR002",
            title: "Invalid Id property type",
            messageFormat: "Type '{0}' has an Id property but it must be of type 'int'",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor IdPropertyNotPublic = new DiagnosticDescriptor(
            id: "GALDR003",
            title: "Id property not public",
            messageFormat: "Type '{0}' has an Id property but both getter and setter must be public",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor ProjectionNotPartial = new DiagnosticDescriptor(
            id: "GALDR020",
            title: "Projection class not partial",
            messageFormat: "Projection type '{0}' must be declared as partial",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor ProjectionSourceNotCollection = new DiagnosticDescriptor(
            id: "GALDR021",
            title: "Source type not a collection",
            messageFormat: "Source type '{0}' for projection '{1}' must have [GaldrDbCollection] attribute",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor ProjectionPropertyNotOnSource = new DiagnosticDescriptor(
            id: "GALDR022",
            title: "Property not on source",
            messageFormat: "Property '{0}' on projection '{1}' does not exist on source type '{2}'",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor ProjectionPropertyTypeMismatch = new DiagnosticDescriptor(
            id: "GALDR023",
            title: "Property type mismatch",
            messageFormat: "Property '{0}' on projection '{1}' has type '{2}' but source '{3}.{0}' has type '{4}'",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        private static readonly DiagnosticDescriptor ProjectionMissingIdProperty = new DiagnosticDescriptor(
            id: "GALDR024",
            title: "Projection missing Id property",
            messageFormat: "Projection '{0}' must include the 'Id' property from source type '{1}'",
            category: "GaldrDb",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true);

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Collect all classes with [GaldrDbCollection] attribute
            IncrementalValuesProvider<ClassInfo> classDeclarations = context.SyntaxProvider
                .ForAttributeWithMetadataName(
                    "GaldrJson.GaldrDbCollectionAttribute",
                    predicate: static (node, _) => node is ClassDeclarationSyntax,
                    transform: static (ctx, _) => GetClassInfo(ctx))
                .Where(static info => info != null)
                .Select(static (info, _) => info!);

            // Collect all classes with [GaldrDbProjection] attribute
            IncrementalValuesProvider<ProjectionInfo> projectionDeclarations = context.SyntaxProvider
                .ForAttributeWithMetadataName(
                    "GaldrJson.GaldrDbProjectionAttribute",
                    predicate: static (node, _) => node is ClassDeclarationSyntax,
                    transform: static (ctx, _) => GetProjectionInfo(ctx))
                .Where(static info => info != null)
                .Select(static (info, _) => info!);

            // Collect both into arrays
            IncrementalValueProvider<ImmutableArray<ClassInfo>> collectedClasses = classDeclarations.Collect();
            IncrementalValueProvider<ImmutableArray<ProjectionInfo>> collectedProjections = projectionDeclarations.Collect();

            // Combine and generate
            IncrementalValueProvider<(ImmutableArray<ClassInfo> Classes, ImmutableArray<ProjectionInfo> Projections)> combined =
                collectedClasses.Combine(collectedProjections);

            context.RegisterSourceOutput(combined, GenerateAllTypes);
        }

        private static ClassInfo GetClassInfo(GeneratorAttributeSyntaxContext context)
        {
            INamedTypeSymbol classSymbol = (INamedTypeSymbol)context.TargetSymbol;

            // Find Id property
            IPropertySymbol idProperty = classSymbol.GetMembers()
                .OfType<IPropertySymbol>()
                .FirstOrDefault(p => p.Name == "Id");

            // Validate Id property exists
            if (idProperty == null)
            {
                context.SemanticModel.Compilation.GetDiagnostics();
                // We report diagnostics in a separate pass
                return new ClassInfo(
                    classSymbol.Name,
                    classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                    GetFullyQualifiedName(classSymbol),
                    null,
                    ImmutableArray<PropertyInfo>.Empty,
                    classSymbol.Locations.FirstOrDefault(),
                    IdValidationResult.Missing);
            }

            // Validate Id property is int
            if (idProperty.Type.SpecialType != SpecialType.System_Int32)
            {
                return new ClassInfo(
                    classSymbol.Name,
                    classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                    GetFullyQualifiedName(classSymbol),
                    null,
                    ImmutableArray<PropertyInfo>.Empty,
                    idProperty.Locations.FirstOrDefault(),
                    IdValidationResult.WrongType);
            }

            // Validate Id property has public getter and setter
            if (idProperty.GetMethod == null ||
                idProperty.SetMethod == null ||
                idProperty.GetMethod.DeclaredAccessibility != Accessibility.Public ||
                idProperty.SetMethod.DeclaredAccessibility != Accessibility.Public)
            {
                return new ClassInfo(
                    classSymbol.Name,
                    classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                    GetFullyQualifiedName(classSymbol),
                    null,
                    ImmutableArray<PropertyInfo>.Empty,
                    idProperty.Locations.FirstOrDefault(),
                    IdValidationResult.NotPublic);
            }

            // Check for [GaldrDbCollection] override
            string collectionOverride = null;
            AttributeData collectionAttr = classSymbol.GetAttributes()
                .FirstOrDefault(a => a.AttributeClass?.Name == "GaldrDbCollectionAttribute");
            if (collectionAttr != null && collectionAttr.ConstructorArguments.Length > 0)
            {
                collectionOverride = collectionAttr.ConstructorArguments[0].Value as string;
            }

            // Collect all public properties with getters
            ImmutableArray<PropertyInfo>.Builder properties = ImmutableArray.CreateBuilder<PropertyInfo>();
            foreach (IPropertySymbol prop in classSymbol.GetMembers().OfType<IPropertySymbol>())
            {
                if (prop.DeclaredAccessibility != Accessibility.Public)
                {
                    continue;
                }
                if (prop.GetMethod == null)
                {
                    continue;
                }

                // Check if property has [GaldrDbIndex] attribute and if it's unique
                bool isIndexed = false;
                bool isUniqueIndex = false;
                AttributeData indexAttr = prop.GetAttributes()
                    .FirstOrDefault(a => a.AttributeClass?.Name == "GaldrDbIndexAttribute");
                
                if (indexAttr != null)
                {
                    isIndexed = true;
                    // Check for Unique = true in named arguments
                    foreach (KeyValuePair<string, TypedConstant> namedArg in indexAttr.NamedArguments)
                    {
                        if (namedArg.Key == "Unique" && namedArg.Value.Value is bool uniqueValue)
                        {
                            isUniqueIndex = uniqueValue;
                        }
                    }
                }

                GaldrFieldTypeInfo fieldType = GetFieldType(prop.Type);
                bool isNestedObject = IsNestedObjectType(prop.Type);
                bool isCollection = IsCollectionType(prop.Type, out ITypeSymbol elementType);
                string collectionElementTypeName = null;
                ImmutableArray<PropertyInfo> nestedProps = ImmutableArray<PropertyInfo>.Empty;

                if (isNestedObject)
                {
                    nestedProps = GetNestedProperties(prop.Type, 1, new HashSet<string>());
                }
                else if (isCollection && elementType != null && IsNestedObjectType(elementType))
                {
                    collectionElementTypeName = GetFullyQualifiedTypeName(elementType);
                    nestedProps = GetNestedProperties(elementType, 1, new HashSet<string>());
                }
                else if (isCollection && elementType != null)
                {
                    collectionElementTypeName = GetFullyQualifiedTypeName(elementType);
                }

                properties.Add(new PropertyInfo(
                    prop.Name,
                    GetFullyQualifiedTypeName(prop.Type),
                    fieldType,
                    isIndexed,
                    isUniqueIndex,
                    isNestedObject,
                    isCollection,
                    collectionElementTypeName,
                    nestedProps));
            }

            return new ClassInfo(
                classSymbol.Name,
                classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                GetFullyQualifiedName(classSymbol),
                collectionOverride,
                properties.ToImmutable(),
                classSymbol.Locations.FirstOrDefault(),
                IdValidationResult.Valid);
        }

        private static ProjectionInfo GetProjectionInfo(GeneratorAttributeSyntaxContext context)
        {
            INamedTypeSymbol classSymbol = (INamedTypeSymbol)context.TargetSymbol;
            ClassDeclarationSyntax classSyntax = (ClassDeclarationSyntax)context.TargetNode;

            // Check if class is partial
            bool isPartial = classSyntax.Modifiers.Any(m => m.Text == "partial");

            // Get the source type from the attribute
            AttributeData projectionAttr = classSymbol.GetAttributes()
                .FirstOrDefault(a => a.AttributeClass?.Name == "GaldrDbProjectionAttribute");

            INamedTypeSymbol sourceType = null;
            if (projectionAttr != null && projectionAttr.ConstructorArguments.Length > 0)
            {
                TypedConstant arg = projectionAttr.ConstructorArguments[0];
                if (arg.Value is INamedTypeSymbol typeSymbol)
                {
                    sourceType = typeSymbol;
                }
            }

            // Check if source type has [GaldrDbCollection] attribute
            bool sourceHasCollectionAttribute = false;
            if (sourceType != null)
            {
                sourceHasCollectionAttribute = sourceType.GetAttributes()
                    .Any(a => a.AttributeClass?.Name == "GaldrDbCollectionAttribute");
            }

            // Get source type properties for validation
            Dictionary<string, IPropertySymbol> sourceProperties = new Dictionary<string, IPropertySymbol>();
            if (sourceType != null)
            {
                foreach (IPropertySymbol prop in sourceType.GetMembers().OfType<IPropertySymbol>())
                {
                    if (prop.DeclaredAccessibility == Accessibility.Public && prop.GetMethod != null)
                    {
                        sourceProperties[prop.Name] = prop;
                    }
                }
            }

            // Collect projection properties and validate against source
            ImmutableArray<PropertyInfo>.Builder properties = ImmutableArray.CreateBuilder<PropertyInfo>();
            ImmutableArray<ProjectionValidationError>.Builder validationErrors = ImmutableArray.CreateBuilder<ProjectionValidationError>();
            bool hasIdProperty = false;

            foreach (IPropertySymbol prop in classSymbol.GetMembers().OfType<IPropertySymbol>())
            {
                if (prop.DeclaredAccessibility != Accessibility.Public)
                {
                    continue;
                }
                if (prop.GetMethod == null)
                {
                    continue;
                }

                if (prop.Name == "Id")
                {
                    hasIdProperty = true;
                }

                // Validate property exists on source
                if (sourceType != null)
                {
                    if (!sourceProperties.TryGetValue(prop.Name, out IPropertySymbol sourceProp))
                    {
                        validationErrors.Add(new ProjectionValidationError(
                            ProjectionValidationErrorKind.PropertyNotOnSource,
                            prop.Name,
                            prop.Locations.FirstOrDefault()));
                    }
                    else if (!SymbolEqualityComparer.Default.Equals(prop.Type, sourceProp.Type))
                    {
                        validationErrors.Add(new ProjectionValidationError(
                            ProjectionValidationErrorKind.PropertyTypeMismatch,
                            prop.Name,
                            prop.Locations.FirstOrDefault(),
                            GetFullyQualifiedTypeName(prop.Type),
                            GetFullyQualifiedTypeName(sourceProp.Type)));
                    }
                }

                GaldrFieldTypeInfo fieldType = GetFieldType(prop.Type);
                properties.Add(new PropertyInfo(
                    prop.Name,
                    GetFullyQualifiedTypeName(prop.Type),
                    fieldType,
                    false,
                    false));
            }

            return new ProjectionInfo(
                classSymbol.Name,
                classSymbol.ContainingNamespace?.ToDisplayString() ?? "",
                GetFullyQualifiedName(classSymbol),
                sourceType != null ? GetFullyQualifiedName(sourceType) : null,
                sourceType?.Name,
                properties.ToImmutable(),
                classSymbol.Locations.FirstOrDefault(),
                isPartial,
                sourceHasCollectionAttribute,
                hasIdProperty,
                validationErrors.ToImmutable());
        }

        private static string GetFullyQualifiedName(INamedTypeSymbol symbol)
        {
            string ns = symbol.ContainingNamespace?.ToDisplayString();
            if (string.IsNullOrEmpty(ns))
            {
                return "global::" + symbol.Name;
            }
            return "global::" + ns + "." + symbol.Name;
        }

        private static string GetFullyQualifiedTypeName(ITypeSymbol type)
        {
            return type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        }

        private static GaldrFieldTypeInfo GetFieldType(ITypeSymbol type)
        {
            // Handle nullable value types
            bool isNullable = false;
            if (type.NullableAnnotation == NullableAnnotation.Annotated && type.IsValueType)
            {
                isNullable = true;
                // Get underlying type for nullable
                if (type is INamedTypeSymbol namedType && namedType.TypeArguments.Length == 1)
                {
                    type = namedType.TypeArguments[0];
                }
            }

            // Check for enums FIRST (before SpecialType switch)
            if (type.TypeKind == TypeKind.Enum)
            {
                INamedTypeSymbol enumSymbol = (INamedTypeSymbol)type;
                INamedTypeSymbol underlyingType = enumSymbol.EnumUnderlyingType;
                return GetEnumFieldType(underlyingType, isNullable);
            }

            switch (type.SpecialType)
            {
                case SpecialType.System_String:
                    return new GaldrFieldTypeInfo("GaldrFieldType.String", "WriteString", isNullable);
                case SpecialType.System_Int32:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Int32", "WriteInt32", isNullable);
                case SpecialType.System_Int64:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Int64", "WriteInt64", isNullable);
                case SpecialType.System_Double:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Double", "WriteDouble", isNullable);
                case SpecialType.System_Boolean:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Boolean", "WriteBoolean", isNullable);
                case SpecialType.System_Decimal:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Decimal", "WriteDecimal", isNullable);
                case SpecialType.System_Byte:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Byte", "WriteByte", isNullable);
                case SpecialType.System_SByte:
                    return new GaldrFieldTypeInfo("GaldrFieldType.SByte", "WriteSByte", isNullable);
                case SpecialType.System_Int16:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Int16", "WriteInt16", isNullable);
                case SpecialType.System_UInt16:
                    return new GaldrFieldTypeInfo("GaldrFieldType.UInt16", "WriteUInt16", isNullable);
                case SpecialType.System_UInt32:
                    return new GaldrFieldTypeInfo("GaldrFieldType.UInt32", "WriteUInt32", isNullable);
                case SpecialType.System_UInt64:
                    return new GaldrFieldTypeInfo("GaldrFieldType.UInt64", "WriteUInt64", isNullable);
                case SpecialType.System_Single:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Single", "WriteSingle", isNullable);
                case SpecialType.System_Char:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Char", "WriteChar", isNullable);
            }

            // Check for known types by name
            string typeName = type.Name;
            string fullName = type.ToDisplayString();

            if (typeName == "DateTime" || fullName == "System.DateTime")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.DateTime", "WriteDateTime", isNullable);
            }
            if (typeName == "DateTimeOffset" || fullName == "System.DateTimeOffset")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.DateTimeOffset", "WriteDateTimeOffset", isNullable);
            }
            if (typeName == "Guid" || fullName == "System.Guid")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.Guid", "WriteGuid", isNullable);
            }
            if (typeName == "TimeSpan" || fullName == "System.TimeSpan")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.TimeSpan", "WriteTimeSpan", isNullable);
            }
            if (typeName == "DateOnly" || fullName == "System.DateOnly")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.DateOnly", "WriteDateOnly", isNullable);
            }
            if (typeName == "TimeOnly" || fullName == "System.TimeOnly")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.TimeOnly", "WriteTimeOnly", isNullable);
            }

            // Non-indexable complex type
            return new GaldrFieldTypeInfo("GaldrFieldType.Complex", null, isNullable);
        }

        private static bool IsCollectionType(ITypeSymbol type, out ITypeSymbol elementType)
        {
            elementType = null;

            // Skip string (which implements IEnumerable<char>)
            if (type.SpecialType == SpecialType.System_String)
            {
                return false;
            }

            // Check for arrays
            if (type is IArrayTypeSymbol arrayType)
            {
                elementType = arrayType.ElementType;
                return true;
            }

            // Check for List<T>, IList<T>, ICollection<T>, IEnumerable<T>
            if (type is INamedTypeSymbol namedType)
            {
                // Check if it's a generic type with one type argument
                if (namedType.IsGenericType && namedType.TypeArguments.Length == 1)
                {
                    string typeName = namedType.ConstructedFrom?.ToDisplayString() ?? "";
                    if (typeName.StartsWith("System.Collections.Generic.List<") ||
                        typeName.StartsWith("System.Collections.Generic.IList<") ||
                        typeName.StartsWith("System.Collections.Generic.ICollection<") ||
                        typeName.StartsWith("System.Collections.Generic.IEnumerable<") ||
                        typeName.StartsWith("System.Collections.Generic.IReadOnlyList<") ||
                        typeName.StartsWith("System.Collections.Generic.IReadOnlyCollection<"))
                    {
                        elementType = namedType.TypeArguments[0];
                        return true;
                    }

                    // Also check the original definition name
                    string defName = namedType.OriginalDefinition?.Name ?? "";
                    if (defName == "List" || defName == "IList" || defName == "ICollection" ||
                        defName == "IEnumerable" || defName == "IReadOnlyList" || defName == "IReadOnlyCollection")
                    {
                        elementType = namedType.TypeArguments[0];
                        return true;
                    }
                }
            }

            return false;
        }

        private static bool IsNestedObjectType(ITypeSymbol type)
        {
            // Skip primitives and well-known types
            if (type.SpecialType != SpecialType.None)
            {
                return false;
            }

            // Skip string
            if (type.Name == "String" || type.ToDisplayString() == "string")
            {
                return false;
            }

            // Skip nullable value types (handled separately)
            if (type.NullableAnnotation == NullableAnnotation.Annotated && type.IsValueType)
            {
                return false;
            }

            // Skip enums
            if (type.TypeKind == TypeKind.Enum)
            {
                return false;
            }

            // Skip well-known struct types
            string typeName = type.Name;
            if (typeName == "DateTime" || typeName == "DateTimeOffset" || typeName == "Guid" ||
                typeName == "TimeSpan" || typeName == "DateOnly" || typeName == "TimeOnly" ||
                typeName == "Decimal")
            {
                return false;
            }

            // Skip collections
            if (IsCollectionType(type, out _))
            {
                return false;
            }

            // Must be a class with public properties
            if (type.TypeKind != TypeKind.Class)
            {
                return false;
            }

            // Check if it has any public properties with getters
            foreach (ISymbol member in type.GetMembers())
            {
                if (member is IPropertySymbol prop &&
                    prop.DeclaredAccessibility == Accessibility.Public &&
                    prop.GetMethod != null)
                {
                    return true;
                }
            }

            return false;
        }

        private static ImmutableArray<PropertyInfo> GetNestedProperties(
            ITypeSymbol type,
            int depth,
            HashSet<string> visited)
        {
            // Limit depth to 3 levels
            if (depth > 3)
            {
                return ImmutableArray<PropertyInfo>.Empty;
            }

            // Prevent infinite recursion for circular references
            string typeKey = type.ToDisplayString();
            if (visited.Contains(typeKey))
            {
                return ImmutableArray<PropertyInfo>.Empty;
            }
            visited.Add(typeKey);

            ImmutableArray<PropertyInfo>.Builder properties = ImmutableArray.CreateBuilder<PropertyInfo>();

            foreach (ISymbol member in type.GetMembers())
            {
                if (member is IPropertySymbol prop &&
                    prop.DeclaredAccessibility == Accessibility.Public &&
                    prop.GetMethod != null)
                {
                    GaldrFieldTypeInfo fieldType = GetFieldType(prop.Type);
                    bool isNestedObject = IsNestedObjectType(prop.Type);
                    bool isCollection = IsCollectionType(prop.Type, out ITypeSymbol elementType);
                    string collectionElementTypeName = null;
                    ImmutableArray<PropertyInfo> nestedProps = ImmutableArray<PropertyInfo>.Empty;

                    if (isNestedObject)
                    {
                        nestedProps = GetNestedProperties(prop.Type, depth + 1, new HashSet<string>(visited));
                    }
                    else if (isCollection && elementType != null && IsNestedObjectType(elementType))
                    {
                        collectionElementTypeName = GetFullyQualifiedTypeName(elementType);
                        nestedProps = GetNestedProperties(elementType, depth + 1, new HashSet<string>(visited));
                    }
                    else if (isCollection && elementType != null)
                    {
                        collectionElementTypeName = GetFullyQualifiedTypeName(elementType);
                    }

                    properties.Add(new PropertyInfo(
                        prop.Name,
                        GetFullyQualifiedTypeName(prop.Type),
                        fieldType,
                        false,
                        false,
                        isNestedObject,
                        isCollection,
                        collectionElementTypeName,
                        nestedProps));
                }
            }

            visited.Remove(typeKey);
            return properties.ToImmutable();
        }

        private static GaldrFieldTypeInfo GetEnumFieldType(INamedTypeSymbol underlyingType, bool isNullable)
        {
            GaldrFieldTypeInfo result;
            switch (underlyingType.SpecialType)
            {
                case SpecialType.System_Byte:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.Byte", "WriteByte", isNullable, true, "byte");
                    break;
                case SpecialType.System_SByte:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.SByte", "WriteSByte", isNullable, true, "sbyte");
                    break;
                case SpecialType.System_Int16:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.Int16", "WriteInt16", isNullable, true, "short");
                    break;
                case SpecialType.System_UInt16:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.UInt16", "WriteUInt16", isNullable, true, "ushort");
                    break;
                case SpecialType.System_Int32:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.Int32", "WriteInt32", isNullable, true, "int");
                    break;
                case SpecialType.System_UInt32:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.UInt32", "WriteUInt32", isNullable, true, "uint");
                    break;
                case SpecialType.System_Int64:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.Int64", "WriteInt64", isNullable, true, "long");
                    break;
                case SpecialType.System_UInt64:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.UInt64", "WriteUInt64", isNullable, true, "ulong");
                    break;
                default:
                    result = new GaldrFieldTypeInfo("GaldrFieldType.Int32", "WriteInt32", isNullable, true, "int");
                    break;
            }

            return result;
        }

        private static void GenerateAllTypes(
            SourceProductionContext context,
            (ImmutableArray<ClassInfo> Classes, ImmutableArray<ProjectionInfo> Projections) input)
        {
            ImmutableArray<ClassInfo> classes = input.Classes;
            ImmutableArray<ProjectionInfo> projections = input.Projections;

            // Report diagnostics for invalid collection types
            if (!classes.IsDefaultOrEmpty)
            {
                foreach (ClassInfo classInfo in classes)
                {
                    switch (classInfo.IdValidation)
                    {
                        case IdValidationResult.Missing:
                            context.ReportDiagnostic(Diagnostic.Create(
                                MissingIdProperty,
                                classInfo.DiagnosticLocation,
                                classInfo.ClassName));
                            break;
                        case IdValidationResult.WrongType:
                            context.ReportDiagnostic(Diagnostic.Create(
                                InvalidIdPropertyType,
                                classInfo.DiagnosticLocation,
                                classInfo.ClassName));
                            break;
                        case IdValidationResult.NotPublic:
                            context.ReportDiagnostic(Diagnostic.Create(
                                IdPropertyNotPublic,
                                classInfo.DiagnosticLocation,
                                classInfo.ClassName));
                            break;
                    }
                }
            }

            // Report diagnostics for invalid projections
            if (!projections.IsDefaultOrEmpty)
            {
                foreach (ProjectionInfo projInfo in projections)
                {
                    if (!projInfo.IsPartial)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(
                            ProjectionNotPartial,
                            projInfo.DiagnosticLocation,
                            projInfo.ClassName));
                    }

                    if (!projInfo.SourceHasCollectionAttribute)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(
                            ProjectionSourceNotCollection,
                            projInfo.DiagnosticLocation,
                            projInfo.SourceClassName ?? "unknown",
                            projInfo.ClassName));
                    }

                    if (!projInfo.HasIdProperty)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(
                            ProjectionMissingIdProperty,
                            projInfo.DiagnosticLocation,
                            projInfo.ClassName,
                            projInfo.SourceClassName ?? "unknown"));
                    }

                    foreach (ProjectionValidationError error in projInfo.ValidationErrors)
                    {
                        switch (error.Kind)
                        {
                            case ProjectionValidationErrorKind.PropertyNotOnSource:
                                context.ReportDiagnostic(Diagnostic.Create(
                                    ProjectionPropertyNotOnSource,
                                    error.Location,
                                    error.PropertyName,
                                    projInfo.ClassName,
                                    projInfo.SourceClassName ?? "unknown"));
                                break;
                            case ProjectionValidationErrorKind.PropertyTypeMismatch:
                                context.ReportDiagnostic(Diagnostic.Create(
                                    ProjectionPropertyTypeMismatch,
                                    error.Location,
                                    error.PropertyName,
                                    projInfo.ClassName,
                                    error.ProjectionPropertyType,
                                    projInfo.SourceClassName ?? "unknown",
                                    error.SourcePropertyType));
                                break;
                        }
                    }
                }
            }

            // Filter to only valid types
            List<ClassInfo> validClasses = classes.IsDefaultOrEmpty
                ? new List<ClassInfo>()
                : classes.Where(c => c.IdValidation == IdValidationResult.Valid).ToList();

            List<ProjectionInfo> validProjections = projections.IsDefaultOrEmpty
                ? new List<ProjectionInfo>()
                : projections.Where(p => p.IsValid).ToList();

            if (validClasses.Count == 0 && validProjections.Count == 0)
            {
                return;
            }

            // Resolve name collisions for classes
            Dictionary<string, string> resolvedNames = new Dictionary<string, string>();
            if (validClasses.Count > 0)
            {
                resolvedNames = ResolveCollisions(validClasses);
            }

            // Resolve name collisions for projections
            Dictionary<string, string> resolvedProjectionNames = new Dictionary<string, string>();
            if (validProjections.Count > 0)
            {
                resolvedProjectionNames = ResolveProjectionCollisions(validProjections);
            }

            // Generate Meta class for each valid collection type
            foreach (ClassInfo classInfo in validClasses)
            {
                string resolvedName = resolvedNames[classInfo.FullyQualifiedName];
                string source = GenerateMetaClass(classInfo, resolvedName);
                context.AddSource($"{resolvedName}Meta.g.cs", source);
            }

            // Generate partial class and Meta class for each valid projection
            foreach (ProjectionInfo projInfo in validProjections)
            {
                string resolvedName = resolvedProjectionNames[projInfo.FullyQualifiedName];
                string sourceResolvedName = resolvedNames.TryGetValue(projInfo.SourceFullyQualifiedName, out string srcName)
                    ? srcName
                    : projInfo.SourceClassName;

                // Generate partial class with IProjectionOf interface
                string partialSource = GenerateProjectionPartialClass(projInfo);
                context.AddSource($"{resolvedName}.g.cs", partialSource);

                // Generate Meta class
                string metaSource = GenerateProjectionMetaClass(projInfo, resolvedName, sourceResolvedName);
                context.AddSource($"{resolvedName}Meta.g.cs", metaSource);
            }

            // Generate the registry
            string registrySource = GenerateRegistry(validClasses, resolvedNames, validProjections, resolvedProjectionNames);
            context.AddSource("GaldrTypeRegistry.g.cs", registrySource);
        }

        private static Dictionary<string, string> ResolveCollisions(List<ClassInfo> classes)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            IEnumerable<IGrouping<string, ClassInfo>> groups = classes.GroupBy(c => c.ClassName);

            foreach (IGrouping<string, ClassInfo> group in groups)
            {
                List<ClassInfo> items = group.ToList();

                if (items.Count == 1)
                {
                    // No collision - use class name directly
                    result[items[0].FullyQualifiedName] = items[0].ClassName;
                }
                else
                {
                    // Collision - resolve by prepending namespace segments
                    ResolveCollisionGroup(items, result);
                }
            }

            return result;
        }

        private static void ResolveCollisionGroup(List<ClassInfo> items, Dictionary<string, string> result)
        {
            int segmentsNeeded = 1;
            int maxSegments = items.Max(c => c.Namespace?.Split('.').Length ?? 0);

            while (segmentsNeeded <= maxSegments + 1)
            {
                List<string> candidateNames = items
                    .Select(c => BuildNameWithSegments(c, segmentsNeeded))
                    .ToList();

                if (candidateNames.Distinct().Count() == items.Count)
                {
                    // All unique - we're done
                    for (int i = 0; i < items.Count; i++)
                    {
                        result[items[i].FullyQualifiedName] = candidateNames[i];
                    }
                    return;
                }

                segmentsNeeded++;
            }

            // Fallback: use full namespace (should never happen)
            foreach (ClassInfo item in items)
            {
                string safeName = item.FullyQualifiedName.Replace(".", "");
                result[item.FullyQualifiedName] = safeName;
            }
        }

        private static string BuildNameWithSegments(ClassInfo classInfo, int segmentCount)
        {
            if (string.IsNullOrEmpty(classInfo.Namespace))
            {
                return classInfo.ClassName;
            }

            string[] segments = classInfo.Namespace.Split('.');
            int startIndex = Math.Max(0, segments.Length - segmentCount);
            StringBuilder sb = new StringBuilder();

            for (int i = startIndex; i < segments.Length; i++)
            {
                sb.Append(segments[i]);
            }
            sb.Append(classInfo.ClassName);

            return sb.ToString();
        }

        private static string GenerateMetaClass(ClassInfo classInfo, string resolvedName)
        {
            IndentedStringBuilder isb = new IndentedStringBuilder();

            // Header
            isb.AppendLine("// <auto-generated/>");
            isb.AppendLine();
            isb.AppendLine("using System;");
            isb.AppendLine("using System.Collections.Generic;");
            isb.AppendLine("using GaldrDbEngine.Query;");
            isb.AppendLine();

            using (isb.Block("namespace GaldrDbEngine.Generated"))
            {
                // Class declaration
                isb.AppendLine("/// <summary>");
                isb.AppendLine($"/// Metadata and field accessors for {classInfo.FullyQualifiedName}.");
                isb.AppendLine("/// </summary>");
                using (isb.Block($"public static class {resolvedName}Meta"))
                {
                    // CollectionName property
                    string collectionName = classInfo.CollectionOverride ?? resolvedName;
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine($"/// The database collection name for {classInfo.ClassName} documents.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public static string CollectionName => \"{collectionName}\";");
                    isb.AppendLine();

                    // IndexedFieldNames property
                    List<string> indexedFields = classInfo.Properties
                        .Where(p => p.IsIndexed)
                        .Select(p => $"\"{p.Name}\"")
                        .ToList();

                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// List of indexed field names for query optimization.");
                    isb.AppendLine("/// </summary>");
                    if (indexedFields.Count == 0)
                    {
                        isb.AppendLine("public static IReadOnlyList<string> IndexedFieldNames { get; } = Array.Empty<string>();");
                    }
                    else
                    {
                        isb.AppendLine($"public static IReadOnlyList<string> IndexedFieldNames {{ get; }} = new string[] {{ {string.Join(", ", indexedFields)} }};");
                    }
                    isb.AppendLine();

                    // UniqueIndexFieldNames property
                    List<string> uniqueIndexedFields = classInfo.Properties
                        .Where(p => p.IsIndexed && p.IsUniqueIndex)
                        .Select(p => $"\"{p.Name}\"")
                        .ToList();

                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// List of unique index field names for constraint enforcement.");
                    isb.AppendLine("/// </summary>");
                    if (uniqueIndexedFields.Count == 0)
                    {
                        isb.AppendLine("public static IReadOnlyList<string> UniqueIndexFieldNames { get; } = Array.Empty<string>();");
                    }
                    else
                    {
                        isb.AppendLine($"public static IReadOnlyList<string> UniqueIndexFieldNames {{ get; }} = new string[] {{ {string.Join(", ", uniqueIndexedFields)} }};");
                    }
                    isb.AppendLine();

                    // GaldrField properties for each property
                    foreach (PropertyInfo prop in classInfo.Properties)
                    {
                        if (prop.IsNestedObject && !prop.NestedProperties.IsDefaultOrEmpty)
                        {
                            // Generate accessor property for nested object
                            string accessorClassName = $"{classInfo.ClassName}{prop.Name}Accessor";
                            isb.AppendLine("/// <summary>");
                            isb.AppendLine($"/// Accessor for nested {prop.Name} properties.");
                            isb.AppendLine("/// </summary>");
                            isb.AppendLine($"public static {accessorClassName} {prop.Name} {{ get; }} = new {accessorClassName}();");
                            isb.AppendLine();
                        }
                        else if (prop.IsCollection && !prop.NestedProperties.IsDefaultOrEmpty)
                        {
                            // Generate collection accessor property
                            string accessorClassName = $"{classInfo.ClassName}{prop.Name}Accessor";
                            isb.AppendLine("/// <summary>");
                            isb.AppendLine($"/// Accessor for collection {prop.Name} element properties.");
                            isb.AppendLine("/// </summary>");
                            isb.AppendLine($"public static {accessorClassName} {prop.Name} {{ get; }} = new {accessorClassName}();");
                            isb.AppendLine();
                        }
                        else
                        {
                            string indexedStr = prop.IsIndexed ? "true" : "false";
                            string indexComment = prop.IsIndexed ? " This field is indexed." : "";

                            isb.AppendLine("/// <summary>");
                            isb.AppendLine($"/// Field accessor for {prop.Name}.{indexComment}");
                            isb.AppendLine("/// </summary>");
                            isb.AppendLine($"public static GaldrField<{classInfo.FullyQualifiedName}, {prop.TypeName}> {prop.Name} {{ get; }} =");
                            using (isb.Indent())
                            {
                                isb.AppendLine($"new GaldrField<{classInfo.FullyQualifiedName}, {prop.TypeName}>(\"{prop.Name}\", {prop.FieldType.FieldTypeEnum}, {indexedStr}, static p => p.{prop.Name});");
                            }
                            isb.AppendLine();
                        }
                    }

                    // Generate nested accessor classes
                    foreach (PropertyInfo prop in classInfo.Properties)
                    {
                        if (prop.IsNestedObject && !prop.NestedProperties.IsDefaultOrEmpty)
                        {
                            GenerateNestedAccessorClass(isb, classInfo, prop, prop.Name);
                        }
                        else if (prop.IsCollection && !prop.NestedProperties.IsDefaultOrEmpty)
                        {
                            GenerateCollectionAccessorClass(isb, classInfo, prop, prop.Name);
                        }
                    }

                    // SetId method
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Sets the Id property on a document instance.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"internal static void SetId({classInfo.FullyQualifiedName} document, int id) => document.Id = id;");
                    isb.AppendLine();

                    // GetId method
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Gets the Id property from a document instance.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"internal static int GetId({classInfo.FullyQualifiedName} document) => document.Id;");
                    isb.AppendLine();

                    // ExtractIndexedFields method
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Extracts indexed field values for index maintenance during insert/update.");
                    isb.AppendLine("/// </summary>");
                    using (isb.Block($"internal static void ExtractIndexedFields({classInfo.FullyQualifiedName} document, IndexFieldWriter writer)"))
                    {
                        foreach (PropertyInfo prop in classInfo.Properties.Where(p => p.IsIndexed))
                        {
                            if (prop.FieldType.WriteMethod != null)
                            {
                                if (prop.FieldType.IsEnum)
                                {
                                    if (prop.FieldType.IsNullable)
                                    {
                                        isb.AppendLine($"writer.{prop.FieldType.WriteMethod}(\"{prop.Name}\", document.{prop.Name}.HasValue ? ({prop.FieldType.EnumCastType}?)document.{prop.Name}.Value : null);");
                                    }
                                    else
                                    {
                                        isb.AppendLine($"writer.{prop.FieldType.WriteMethod}(\"{prop.Name}\", ({prop.FieldType.EnumCastType})document.{prop.Name});");
                                    }
                                }
                                else
                                {
                                    isb.AppendLine($"writer.{prop.FieldType.WriteMethod}(\"{prop.Name}\", document.{prop.Name});");
                                }
                            }
                        }
                    }
                    isb.AppendLine();

                    // TypeInfo property
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Type info instance for registry.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public static GaldrTypeInfo<{classInfo.FullyQualifiedName}> TypeInfo {{ get; }} = new GaldrTypeInfo<{classInfo.FullyQualifiedName}>(");
                    using (isb.Indent())
                    {
                        isb.AppendLine("collectionName: CollectionName,");
                        isb.AppendLine("indexedFieldNames: IndexedFieldNames,");
                        isb.AppendLine("uniqueIndexFieldNames: UniqueIndexFieldNames,");
                        isb.AppendLine("idSetter: SetId,");
                        isb.AppendLine("idGetter: GetId,");
                        isb.AppendLine("extractIndexedFields: ExtractIndexedFields);");
                    }
                }
            }

            return isb.ToString();
        }

        private static void GenerateNestedAccessorClass(
            IndentedStringBuilder isb,
            ClassInfo classInfo,
            PropertyInfo prop,
            string pathPrefix)
        {
            string accessorClassName = $"{classInfo.ClassName}{prop.Name}Accessor";

            isb.AppendLine();
            isb.AppendLine("/// <summary>");
            isb.AppendLine($"/// Provides access to nested properties of {prop.Name}.");
            isb.AppendLine("/// </summary>");
            using (isb.Block($"public sealed class {accessorClassName}"))
            {
                foreach (PropertyInfo nestedProp in prop.NestedProperties)
                {
                    string fullPath = $"{pathPrefix}.{nestedProp.Name}";

                    if (nestedProp.IsNestedObject && !nestedProp.NestedProperties.IsDefaultOrEmpty)
                    {
                        string nestedAccessorClassName = $"{classInfo.ClassName}{prop.Name}{nestedProp.Name}Accessor";
                        isb.AppendLine("/// <summary>");
                        isb.AppendLine($"/// Accessor for nested {nestedProp.Name} properties.");
                        isb.AppendLine("/// </summary>");
                        isb.AppendLine($"public {nestedAccessorClassName} {nestedProp.Name} {{ get; }} = new {nestedAccessorClassName}();");
                        isb.AppendLine();
                    }
                    else
                    {
                        isb.AppendLine("/// <summary>");
                        isb.AppendLine($"/// Field accessor for {fullPath}.");
                        isb.AppendLine("/// </summary>");
                        isb.AppendLine($"public GaldrField<{classInfo.FullyQualifiedName}, {nestedProp.TypeName}> {nestedProp.Name} {{ get; }} =");
                        using (isb.Indent())
                        {
                            string accessorSuffix = IsValueTypeField(nestedProp.FieldType) ? " ?? default" : "";
                            isb.AppendLine($"new GaldrField<{classInfo.FullyQualifiedName}, {nestedProp.TypeName}>(\"{fullPath}\", {nestedProp.FieldType.FieldTypeEnum}, false, static p => p.{pathPrefix}?.{nestedProp.Name}{accessorSuffix});");
                        }
                        isb.AppendLine();
                    }
                }
            }

            foreach (PropertyInfo nestedProp in prop.NestedProperties)
            {
                if (nestedProp.IsNestedObject && !nestedProp.NestedProperties.IsDefaultOrEmpty)
                {
                    GenerateDeepNestedAccessorClass(isb, classInfo, prop.Name, nestedProp, $"{pathPrefix}.{nestedProp.Name}");
                }
            }
        }

        private static bool IsValueTypeField(GaldrFieldTypeInfo fieldType)
        {
            string enumType = fieldType.FieldTypeEnum;
            return enumType == "GaldrFieldType.Int32" ||
                   enumType == "GaldrFieldType.Int64" ||
                   enumType == "GaldrFieldType.Int16" ||
                   enumType == "GaldrFieldType.Byte" ||
                   enumType == "GaldrFieldType.SByte" ||
                   enumType == "GaldrFieldType.UInt16" ||
                   enumType == "GaldrFieldType.UInt32" ||
                   enumType == "GaldrFieldType.UInt64" ||
                   enumType == "GaldrFieldType.Single" ||
                   enumType == "GaldrFieldType.Double" ||
                   enumType == "GaldrFieldType.Decimal" ||
                   enumType == "GaldrFieldType.Boolean" ||
                   enumType == "GaldrFieldType.DateTime" ||
                   enumType == "GaldrFieldType.DateTimeOffset" ||
                   enumType == "GaldrFieldType.DateOnly" ||
                   enumType == "GaldrFieldType.TimeOnly" ||
                   enumType == "GaldrFieldType.TimeSpan" ||
                   enumType == "GaldrFieldType.Guid" ||
                   enumType == "GaldrFieldType.Char" ||
                   fieldType.IsEnum;
        }

        private static void GenerateDeepNestedAccessorClass(
            IndentedStringBuilder isb,
            ClassInfo classInfo,
            string parentName,
            PropertyInfo prop,
            string pathPrefix)
        {
            string accessorClassName = $"{classInfo.ClassName}{parentName}{prop.Name}Accessor";

            isb.AppendLine();
            isb.AppendLine("/// <summary>");
            isb.AppendLine($"/// Provides access to deeply nested properties of {pathPrefix}.");
            isb.AppendLine("/// </summary>");
            using (isb.Block($"public sealed class {accessorClassName}"))
            {
                foreach (PropertyInfo nestedProp in prop.NestedProperties)
                {
                    string fullPath = $"{pathPrefix}.{nestedProp.Name}";

                    isb.AppendLine("/// <summary>");
                    isb.AppendLine($"/// Field accessor for {fullPath}.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public GaldrField<{classInfo.FullyQualifiedName}, {nestedProp.TypeName}> {nestedProp.Name} {{ get; }} =");
                    using (isb.Indent())
                    {
                        string accessorSuffix = IsValueTypeField(nestedProp.FieldType) ? " ?? default" : "";
                        isb.AppendLine($"new GaldrField<{classInfo.FullyQualifiedName}, {nestedProp.TypeName}>(\"{fullPath}\", {nestedProp.FieldType.FieldTypeEnum}, false, static p => p.{pathPrefix}?.{nestedProp.Name}{accessorSuffix});");
                    }
                    isb.AppendLine();
                }
            }
        }

        private static void GenerateCollectionAccessorClass(
            IndentedStringBuilder isb,
            ClassInfo classInfo,
            PropertyInfo prop,
            string pathPrefix)
        {
            string accessorClassName = $"{classInfo.ClassName}{prop.Name}Accessor";
            string elementTypeName = prop.CollectionElementTypeName;

            isb.AppendLine();
            isb.AppendLine("/// <summary>");
            isb.AppendLine($"/// Provides access to element properties of {prop.Name} collection for any-match queries.");
            isb.AppendLine("/// </summary>");
            using (isb.Block($"public sealed class {accessorClassName}"))
            {
                foreach (PropertyInfo nestedProp in prop.NestedProperties)
                {
                    string fullPath = $"{pathPrefix}.{nestedProp.Name}";

                    isb.AppendLine("/// <summary>");
                    isb.AppendLine($"/// Collection field accessor for {fullPath}.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public GaldrCollectionField<{classInfo.FullyQualifiedName}, {elementTypeName}, {nestedProp.TypeName}> {nestedProp.Name} {{ get; }} =");
                    using (isb.Indent())
                    {
                        isb.AppendLine($"new GaldrCollectionField<{classInfo.FullyQualifiedName}, {elementTypeName}, {nestedProp.TypeName}>(\"{fullPath}\", {nestedProp.FieldType.FieldTypeEnum}, false,");
                        isb.AppendLine($"static p => p.{pathPrefix} ?? System.Linq.Enumerable.Empty<{elementTypeName}>(),");
                        isb.AppendLine($"static e => e.{nestedProp.Name});");
                    }
                    isb.AppendLine();
                }
            }
        }

        private static Dictionary<string, string> ResolveProjectionCollisions(List<ProjectionInfo> projections)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            IEnumerable<IGrouping<string, ProjectionInfo>> groups = projections.GroupBy(p => p.ClassName);

            foreach (IGrouping<string, ProjectionInfo> group in groups)
            {
                List<ProjectionInfo> items = group.ToList();

                if (items.Count == 1)
                {
                    result[items[0].FullyQualifiedName] = items[0].ClassName;
                }
                else
                {
                    ResolveProjectionCollisionGroup(items, result);
                }
            }

            return result;
        }

        private static void ResolveProjectionCollisionGroup(List<ProjectionInfo> items, Dictionary<string, string> result)
        {
            int segmentsNeeded = 1;
            int maxSegments = items.Max(p => p.Namespace?.Split('.').Length ?? 0);

            while (segmentsNeeded <= maxSegments + 1)
            {
                List<string> candidateNames = items
                    .Select(p => BuildProjectionNameWithSegments(p, segmentsNeeded))
                    .ToList();

                if (candidateNames.Distinct().Count() == items.Count)
                {
                    for (int i = 0; i < items.Count; i++)
                    {
                        result[items[i].FullyQualifiedName] = candidateNames[i];
                    }
                    return;
                }

                segmentsNeeded++;
            }

            foreach (ProjectionInfo item in items)
            {
                string safeName = item.FullyQualifiedName.Replace(".", "");
                result[item.FullyQualifiedName] = safeName;
            }
        }

        private static string BuildProjectionNameWithSegments(ProjectionInfo projInfo, int segmentCount)
        {
            if (string.IsNullOrEmpty(projInfo.Namespace))
            {
                return projInfo.ClassName;
            }

            string[] segments = projInfo.Namespace.Split('.');
            int startIndex = Math.Max(0, segments.Length - segmentCount);
            StringBuilder sb = new StringBuilder();

            for (int i = startIndex; i < segments.Length; i++)
            {
                sb.Append(segments[i]);
            }
            sb.Append(projInfo.ClassName);

            return sb.ToString();
        }

        private static string GenerateProjectionPartialClass(ProjectionInfo projInfo)
        {
            IndentedStringBuilder isb = new IndentedStringBuilder();

            isb.AppendLine("// <auto-generated/>");
            isb.AppendLine();
            isb.AppendLine("using GaldrDbEngine.Query;");
            isb.AppendLine();

            if (!string.IsNullOrEmpty(projInfo.Namespace))
            {
                using (isb.Block($"namespace {projInfo.Namespace}"))
                {
                    using (isb.Block($"public partial class {projInfo.ClassName} : IProjectionOf<{projInfo.SourceFullyQualifiedName}>"))
                    {
                    }
                }
            }
            else
            {
                using (isb.Block($"public partial class {projInfo.ClassName} : IProjectionOf<{projInfo.SourceFullyQualifiedName}>"))
                {
                }
            }

            return isb.ToString();
        }

        private static string GenerateProjectionMetaClass(ProjectionInfo projInfo, string resolvedName, string sourceResolvedName)
        {
            IndentedStringBuilder isb = new IndentedStringBuilder();

            isb.AppendLine("// <auto-generated/>");
            isb.AppendLine();
            isb.AppendLine("using System;");
            isb.AppendLine("using System.Collections.Generic;");
            isb.AppendLine("using GaldrDbEngine.Query;");
            isb.AppendLine();

            using (isb.Block("namespace GaldrDbEngine.Generated"))
            {
                isb.AppendLine("/// <summary>");
                isb.AppendLine($"/// Metadata and field accessors for projection {projInfo.FullyQualifiedName}.");
                isb.AppendLine("/// </summary>");
                using (isb.Block($"public static class {resolvedName}Meta"))
                {
                    // CollectionName - delegates to source
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine($"/// The database collection name (from source type {projInfo.SourceClassName}).");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public static string CollectionName => {sourceResolvedName}Meta.CollectionName;");
                    isb.AppendLine();

                    // SourceType
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// The source type this projection is based on.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public static Type SourceType => typeof({projInfo.SourceFullyQualifiedName});");
                    isb.AppendLine();

                    // IndexedFieldNames - projections don't have their own indexes
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Projections do not have their own indexes.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine("public static IReadOnlyList<string> IndexedFieldNames { get; } = Array.Empty<string>();");
                    isb.AppendLine();

                    // UniqueIndexFieldNames
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Projections do not have their own unique indexes.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine("public static IReadOnlyList<string> UniqueIndexFieldNames { get; } = Array.Empty<string>();");
                    isb.AppendLine();

                    // GaldrField properties for each property
                    foreach (PropertyInfo prop in projInfo.Properties)
                    {
                        isb.AppendLine("/// <summary>");
                        isb.AppendLine($"/// Field accessor for {prop.Name}.");
                        isb.AppendLine("/// </summary>");
                        isb.AppendLine($"public static GaldrField<{projInfo.FullyQualifiedName}, {prop.TypeName}> {prop.Name} {{ get; }} =");
                        using (isb.Indent())
                        {
                            isb.AppendLine($"new GaldrField<{projInfo.FullyQualifiedName}, {prop.TypeName}>(\"{prop.Name}\", {prop.FieldType.FieldTypeEnum}, false, static p => p.{prop.Name});");
                        }
                        isb.AppendLine();
                    }

                    // TypeInfo property with inline converter lambda
                    isb.AppendLine("/// <summary>");
                    isb.AppendLine("/// Type info instance for registry.");
                    isb.AppendLine("/// </summary>");
                    isb.AppendLine($"public static GaldrProjectionTypeInfo<{projInfo.FullyQualifiedName}, {projInfo.SourceFullyQualifiedName}> TypeInfo {{ get; }} =");
                    using (isb.Indent())
                    {
                        isb.AppendLine($"new GaldrProjectionTypeInfo<{projInfo.FullyQualifiedName}, {projInfo.SourceFullyQualifiedName}>(");
                        using (isb.Indent())
                        {
                            isb.AppendLine("collectionName: CollectionName,");
                            isb.AppendLine("indexedFieldNames: IndexedFieldNames,");
                            isb.AppendLine("uniqueIndexFieldNames: UniqueIndexFieldNames,");
                            isb.AppendLine($"converter: static source => new {projInfo.FullyQualifiedName}");
                            isb.AppendLine("{");
                            using (isb.Indent())
                            {
                                foreach (PropertyInfo prop in projInfo.Properties)
                                {
                                    isb.AppendLine($"{prop.Name} = source.{prop.Name},");
                                }
                            }
                            isb.AppendLine("},");
                            isb.AppendLine($"sourceIdGetter: {sourceResolvedName}Meta.GetId);");
                        }
                    }
                }
            }

            return isb.ToString();
        }

        private static string GenerateRegistry(
            List<ClassInfo> classes,
            Dictionary<string, string> resolvedNames,
            List<ProjectionInfo> projections,
            Dictionary<string, string> resolvedProjectionNames)
        {
            IndentedStringBuilder isb = new IndentedStringBuilder();

            isb.AppendLine("// <auto-generated/>");
            isb.AppendLine();
            isb.AppendLine("using System.Runtime.CompilerServices;");
            isb.AppendLine("using GaldrDbEngine.Query;");
            isb.AppendLine();

            using (isb.Block("namespace GaldrDbEngine.Generated"))
            {
                isb.AppendLine("/// <summary>");
                isb.AppendLine("/// Module initializer that registers all GaldrDb types with the GaldrTypeRegistry.");
                isb.AppendLine("/// </summary>");
                using (isb.Block("internal static class GaldrTypeRegistryInitializer"))
                {
                    isb.AppendLine("[ModuleInitializer]");
                    using (isb.Block("internal static void Initialize()"))
                    {
                        // Register collection types
                        foreach (ClassInfo classInfo in classes)
                        {
                            string resolvedName = resolvedNames[classInfo.FullyQualifiedName];
                            isb.AppendLine($"global::GaldrDbEngine.Query.GaldrTypeRegistry.Register({resolvedName}Meta.TypeInfo);");
                        }

                        // Register projection types
                        foreach (ProjectionInfo projInfo in projections)
                        {
                            string resolvedName = resolvedProjectionNames[projInfo.FullyQualifiedName];
                            isb.AppendLine($"global::GaldrDbEngine.Query.GaldrTypeRegistry.Register({resolvedName}Meta.TypeInfo);");
                        }
                    }
                }
            }

            return isb.ToString();
        }
    }
}
