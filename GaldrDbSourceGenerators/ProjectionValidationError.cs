using Microsoft.CodeAnalysis;

namespace GaldrDbSourceGenerators
{
    internal enum ProjectionValidationErrorKind
    {
        PropertyNotOnSource,
        PropertyTypeMismatch
    }

    internal sealed class ProjectionValidationError
    {
        public ProjectionValidationErrorKind Kind { get; }
        public string PropertyName { get; }
        public Location Location { get; }
        public string ProjectionPropertyType { get; }
        public string SourcePropertyType { get; }

        public ProjectionValidationError(
            ProjectionValidationErrorKind kind,
            string propertyName,
            Location location,
            string projectionPropertyType = null,
            string sourcePropertyType = null)
        {
            Kind = kind;
            PropertyName = propertyName;
            Location = location;
            ProjectionPropertyType = projectionPropertyType;
            SourcePropertyType = sourcePropertyType;
        }
    }
}
