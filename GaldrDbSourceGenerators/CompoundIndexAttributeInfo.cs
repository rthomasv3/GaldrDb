using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace GaldrDbSourceGenerators
{
    internal sealed class CompoundIndexAttributeInfo
    {
        public ImmutableArray<string> FieldNames { get; }
        public bool IsUnique { get; }
        public Location Location { get; }

        public CompoundIndexAttributeInfo(ImmutableArray<string> fieldNames, bool isUnique, Location location)
        {
            FieldNames = fieldNames;
            IsUnique = isUnique;
            Location = location;
        }
    }
}
