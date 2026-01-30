using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace GaldrDbSourceGenerators
{
    internal sealed class ClassInfo
    {
        public string ClassName { get; }
        public string Namespace { get; }
        public string FullyQualifiedName { get; }
        public string CollectionOverride { get; }
        public ImmutableArray<PropertyInfo> Properties { get; }
        public ImmutableArray<CompoundIndexAttributeInfo> CompoundIndexes { get; }
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
            CompoundIndexes = ImmutableArray<CompoundIndexAttributeInfo>.Empty;
            DiagnosticLocation = diagnosticLocation;
            IdValidation = idValidation;
        }

        public ClassInfo(
            string className,
            string @namespace,
            string fullyQualifiedName,
            string collectionOverride,
            ImmutableArray<PropertyInfo> properties,
            ImmutableArray<CompoundIndexAttributeInfo> compoundIndexes,
            Location diagnosticLocation,
            IdValidationResult idValidation)
        {
            ClassName = className;
            Namespace = @namespace;
            FullyQualifiedName = fullyQualifiedName;
            CollectionOverride = collectionOverride;
            Properties = properties;
            CompoundIndexes = compoundIndexes;
            DiagnosticLocation = diagnosticLocation;
            IdValidation = idValidation;
        }
    }
}
