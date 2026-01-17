namespace GaldrDbEngine.Query;

/// <summary>
/// Marker interface indicating a type is a projection of a source document type.
/// </summary>
/// <typeparam name="TSource">The source document type.</typeparam>
public interface IProjectionOf<TSource>
{
}
