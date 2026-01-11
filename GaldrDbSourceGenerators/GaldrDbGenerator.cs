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

        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            // Collect all classes with [GaldrJsonSerializable] attribute
            IncrementalValuesProvider<ClassInfo> classDeclarations = context.SyntaxProvider
                .ForAttributeWithMetadataName(
                    "GaldrJson.GaldrDbCollectionAttribute",
                    predicate: static (node, _) => node is ClassDeclarationSyntax,
                    transform: static (ctx, _) => GetClassInfo(ctx))
                .Where(static info => info != null)
                .Select(static (info, _) => info!);

            // Collect into array and generate
            IncrementalValueProvider<ImmutableArray<ClassInfo>> collected = classDeclarations.Collect();

            context.RegisterSourceOutput(collected, GenerateAllTypes);
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

                properties.Add(new PropertyInfo(
                    prop.Name,
                    GetFullyQualifiedTypeName(prop.Type),
                    fieldType,
                    isIndexed,
                    isUniqueIndex));
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
            if (type.NullableAnnotation == NullableAnnotation.Annotated && type.IsValueType)
            {
                // Get underlying type for nullable
                if (type is INamedTypeSymbol namedType && namedType.TypeArguments.Length == 1)
                {
                    type = namedType.TypeArguments[0];
                }
            }

            switch (type.SpecialType)
            {
                case SpecialType.System_String:
                    return new GaldrFieldTypeInfo("GaldrFieldType.String", "WriteString");
                case SpecialType.System_Int32:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Int32", "WriteInt32");
                case SpecialType.System_Int64:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Int64", "WriteInt64");
                case SpecialType.System_Double:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Double", "WriteDouble");
                case SpecialType.System_Boolean:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Boolean", "WriteBoolean");
                case SpecialType.System_Decimal:
                    return new GaldrFieldTypeInfo("GaldrFieldType.Decimal", "WriteDecimal");
            }

            // Check for known types by name
            string typeName = type.Name;
            string fullName = type.ToDisplayString();

            if (typeName == "DateTime" || fullName == "System.DateTime")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.DateTime", "WriteDateTime");
            }
            if (typeName == "DateTimeOffset" || fullName == "System.DateTimeOffset")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.DateTimeOffset", "WriteDateTimeOffset");
            }
            if (typeName == "Guid" || fullName == "System.Guid")
            {
                return new GaldrFieldTypeInfo("GaldrFieldType.Guid", "WriteGuid");
            }

            // Non-indexable complex type
            return new GaldrFieldTypeInfo("GaldrFieldType.Complex", null);
        }

        private static void GenerateAllTypes(SourceProductionContext context, ImmutableArray<ClassInfo> classes)
        {
            if (classes.IsDefaultOrEmpty)
            {
                return;
            }

            // First, report any diagnostics for invalid types
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

            // Filter to only valid types
            List<ClassInfo> validClasses = classes.Where(c => c.IdValidation == IdValidationResult.Valid).ToList();

            if (validClasses.Count == 0)
            {
                return;
            }

            // Resolve name collisions
            Dictionary<string, string> resolvedNames = ResolveCollisions(validClasses);

            // Generate Meta class for each valid type
            foreach (ClassInfo classInfo in validClasses)
            {
                string resolvedName = resolvedNames[classInfo.FullyQualifiedName];
                string source = GenerateMetaClass(classInfo, resolvedName);
                context.AddSource($"{resolvedName}Meta.g.cs", source);
            }

            // Generate the registry
            string registrySource = GenerateRegistry(validClasses, resolvedNames);
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
            StringBuilder sb = new StringBuilder();

            // Header
            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine();
            sb.AppendLine("using System;");
            sb.AppendLine("using System.Collections.Generic;");
            sb.AppendLine("using GaldrDbEngine.Query;");
            sb.AppendLine();
            sb.AppendLine("namespace GaldrDbEngine.Generated");
            sb.AppendLine("{");

            // Class declaration
            sb.AppendLine($"    /// <summary>");
            sb.AppendLine($"    /// Metadata and field accessors for {classInfo.FullyQualifiedName}.");
            sb.AppendLine($"    /// </summary>");
            sb.AppendLine($"    public static class {resolvedName}Meta");
            sb.AppendLine("    {");

            // CollectionName property
            string collectionName = classInfo.CollectionOverride ?? resolvedName;
            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// The database collection name for {classInfo.ClassName} documents.");
            sb.AppendLine($"        /// </summary>");
            sb.AppendLine($"        public static string CollectionName => \"{collectionName}\";");
            sb.AppendLine();

            // IndexedFieldNames property
            List<string> indexedFields = classInfo.Properties
                .Where(p => p.IsIndexed)
                .Select(p => $"\"{p.Name}\"")
                .ToList();

            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// List of indexed field names for query optimization.");
            sb.AppendLine($"        /// </summary>");
            if (indexedFields.Count == 0)
            {
                sb.AppendLine($"        public static IReadOnlyList<string> IndexedFieldNames {{ get; }} = Array.Empty<string>();");
            }
            else
            {
                sb.AppendLine($"        public static IReadOnlyList<string> IndexedFieldNames {{ get; }} = new string[] {{ {string.Join(", ", indexedFields)} }};");
            }
            sb.AppendLine();

            // UniqueIndexFieldNames property
            List<string> uniqueIndexedFields = classInfo.Properties
                .Where(p => p.IsIndexed && p.IsUniqueIndex)
                .Select(p => $"\"{p.Name}\"")
                .ToList();

            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// List of unique index field names for constraint enforcement.");
            sb.AppendLine($"        /// </summary>");
            if (uniqueIndexedFields.Count == 0)
            {
                sb.AppendLine($"        public static IReadOnlyList<string> UniqueIndexFieldNames {{ get; }} = Array.Empty<string>();");
            }
            else
            {
                sb.AppendLine($"        public static IReadOnlyList<string> UniqueIndexFieldNames {{ get; }} = new string[] {{ {string.Join(", ", uniqueIndexedFields)} }};");
            }
            sb.AppendLine();

            // GaldrField properties for each property
            foreach (PropertyInfo prop in classInfo.Properties)
            {
                string indexedStr = prop.IsIndexed ? "true" : "false";
                string indexComment = prop.IsIndexed ? " This field is indexed." : "";

                sb.AppendLine($"        /// <summary>");
                sb.AppendLine($"        /// Field accessor for {prop.Name}.{indexComment}");
                sb.AppendLine($"        /// </summary>");
                sb.AppendLine($"        public static GaldrField<{classInfo.FullyQualifiedName}, {prop.TypeName}> {prop.Name} {{ get; }} =");
                sb.AppendLine($"            new GaldrField<{classInfo.FullyQualifiedName}, {prop.TypeName}>(\"{prop.Name}\", {prop.FieldType.FieldTypeEnum}, {indexedStr}, static p => p.{prop.Name});");
                sb.AppendLine();
            }

            // SetId method
            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// Sets the Id property on a document instance.");
            sb.AppendLine($"        /// </summary>");
            sb.AppendLine($"        internal static void SetId({classInfo.FullyQualifiedName} document, int id) => document.Id = id;");
            sb.AppendLine();

            // GetId method
            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// Gets the Id property from a document instance.");
            sb.AppendLine($"        /// </summary>");
            sb.AppendLine($"        internal static int GetId({classInfo.FullyQualifiedName} document) => document.Id;");
            sb.AppendLine();

            // ExtractIndexedFields method
            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// Extracts indexed field values for index maintenance during insert/update.");
            sb.AppendLine($"        /// </summary>");
            sb.AppendLine($"        internal static void ExtractIndexedFields({classInfo.FullyQualifiedName} document, IndexFieldWriter writer)");
            sb.AppendLine("        {");

            foreach (PropertyInfo prop in classInfo.Properties.Where(p => p.IsIndexed))
            {
                if (prop.FieldType.WriteMethod != null)
                {
                    sb.AppendLine($"            writer.{prop.FieldType.WriteMethod}(\"{prop.Name}\", document.{prop.Name});");
                }
            }

            sb.AppendLine("        }");
            sb.AppendLine();

            // TypeInfo property
            sb.AppendLine($"        /// <summary>");
            sb.AppendLine($"        /// Type info instance for registry.");
            sb.AppendLine($"        /// </summary>");
            sb.AppendLine($"        public static GaldrTypeInfo<{classInfo.FullyQualifiedName}> TypeInfo {{ get; }} = new GaldrTypeInfo<{classInfo.FullyQualifiedName}>(");
            sb.AppendLine($"            collectionName: CollectionName,");
            sb.AppendLine($"            indexedFieldNames: IndexedFieldNames,");
            sb.AppendLine($"            uniqueIndexFieldNames: UniqueIndexFieldNames,");
            sb.AppendLine($"            idSetter: SetId,");
            sb.AppendLine($"            idGetter: GetId,");
            sb.AppendLine($"            extractIndexedFields: ExtractIndexedFields);");

            sb.AppendLine("    }");
            sb.AppendLine("}");

            return sb.ToString();
        }

        private static string GenerateRegistry(List<ClassInfo> classes, Dictionary<string, string> resolvedNames)
        {
            StringBuilder sb = new StringBuilder();

            // Header
            sb.AppendLine("// <auto-generated/>");
            sb.AppendLine();
            sb.AppendLine("using System.Runtime.CompilerServices;");
            sb.AppendLine("using GaldrDbEngine.Query;");
            sb.AppendLine();
            sb.AppendLine("namespace GaldrDbEngine.Generated");
            sb.AppendLine("{");
            sb.AppendLine("    /// <summary>");
            sb.AppendLine("    /// Module initializer that registers all GaldrJsonSerializable types with the GaldrTypeRegistry.");
            sb.AppendLine("    /// </summary>");
            sb.AppendLine("    internal static class GaldrTypeRegistryInitializer");
            sb.AppendLine("    {");
            sb.AppendLine("        [ModuleInitializer]");
            sb.AppendLine("        internal static void Initialize()");
            sb.AppendLine("        {");

            foreach (ClassInfo classInfo in classes)
            {
                string resolvedName = resolvedNames[classInfo.FullyQualifiedName];
                sb.AppendLine($"            global::GaldrDbEngine.Query.GaldrTypeRegistry.Register({resolvedName}Meta.TypeInfo);");
            }

            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");

            return sb.ToString();
        }
    }

    internal enum IdValidationResult
    {
        Valid,
        Missing,
        WrongType,
        NotPublic
    }

    internal sealed class ClassInfo
    {
        public string ClassName { get; }
        public string Namespace { get; }
        public string FullyQualifiedName { get; }
        public string CollectionOverride { get; }
        public ImmutableArray<PropertyInfo> Properties { get; }
        public Location DiagnosticLocation { get; }
        public IdValidationResult IdValidation { get; }

        public ClassInfo(
            string className,
            string @namespace,
            string fullyQualifiedName,
            string collectionOverride,
            ImmutableArray<PropertyInfo> properties,
            Location diagnosticLocation,
            IdValidationResult idValidation)
        {
            ClassName = className;
            Namespace = @namespace;
            FullyQualifiedName = fullyQualifiedName;
            CollectionOverride = collectionOverride;
            Properties = properties;
            DiagnosticLocation = diagnosticLocation;
            IdValidation = idValidation;
        }
    }

    internal sealed class PropertyInfo
    {
        public string Name { get; }
        public string TypeName { get; }
        public GaldrFieldTypeInfo FieldType { get; }
        public bool IsIndexed { get; }
        public bool IsUniqueIndex { get; }

        public PropertyInfo(string name, string typeName, GaldrFieldTypeInfo fieldType, bool isIndexed, bool isUniqueIndex)
        {
            Name = name;
            TypeName = typeName;
            FieldType = fieldType;
            IsIndexed = isIndexed;
            IsUniqueIndex = isUniqueIndex;
        }
    }

    internal sealed class GaldrFieldTypeInfo
    {
        public string FieldTypeEnum { get; }
        public string WriteMethod { get; }

        public GaldrFieldTypeInfo(string fieldTypeEnum, string writeMethod)
        {
            FieldTypeEnum = fieldTypeEnum;
            WriteMethod = writeMethod;
        }
    }
}
