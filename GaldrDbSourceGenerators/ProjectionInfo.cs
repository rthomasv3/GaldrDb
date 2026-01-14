using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace GaldrDbSourceGenerators
{
    internal sealed class ProjectionInfo
    {
        public string ClassName { get; }
        public string Namespace { get; }
        public string FullyQualifiedName { get; }
        public string SourceFullyQualifiedName { get; }
        public string SourceClassName { get; }
        public ImmutableArray<PropertyInfo> Properties { get; }
        public Location DiagnosticLocation { get; }
        public bool IsPartial { get; }
        public bool SourceHasCollectionAttribute { get; }
        public bool HasIdProperty { get; }
        public ImmutableArray<ProjectionValidationError> ValidationErrors { get; }

        public ProjectionInfo(
            string className,
            string @namespace,
            string fullyQualifiedName,
            string sourceFullyQualifiedName,
            string sourceClassName,
            ImmutableArray<PropertyInfo> properties,
            Location diagnosticLocation,
            bool isPartial,
            bool sourceHasCollectionAttribute,
            bool hasIdProperty,
            ImmutableArray<ProjectionValidationError> validationErrors)
        {
            ClassName = className;
            Namespace = @namespace;
            FullyQualifiedName = fullyQualifiedName;
            SourceFullyQualifiedName = sourceFullyQualifiedName;
            SourceClassName = sourceClassName;
            Properties = properties;
            DiagnosticLocation = diagnosticLocation;
            IsPartial = isPartial;
            SourceHasCollectionAttribute = sourceHasCollectionAttribute;
            HasIdProperty = hasIdProperty;
            ValidationErrors = validationErrors;
        }

        public bool IsValid
        {
            get
            {
                return IsPartial &&
                       SourceHasCollectionAttribute &&
                       HasIdProperty &&
                       ValidationErrors.IsEmpty;
            }
        }
    }
}
