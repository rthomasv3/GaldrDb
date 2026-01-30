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
}
