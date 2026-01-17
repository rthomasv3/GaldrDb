using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

/// <summary>
/// Global registry of document type metadata. Populated by source generators.
/// </summary>
public static class GaldrTypeRegistry
{
    private static readonly Dictionary<Type, IGaldrTypeInfo> _registry = new Dictionary<Type, IGaldrTypeInfo>();
    private static bool _initialized;

    /// <summary>
    /// Registers type metadata. Called by source-generated code.
    /// </summary>
    /// <param name="typeInfo">The type info to register.</param>
    public static void Register(IGaldrTypeInfo typeInfo)
    {
        _registry[typeInfo.DocumentType] = typeInfo;
        _initialized = true;
    }

    /// <summary>
    /// Gets the type info for a document type.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <returns>The type info.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the type is not registered.</exception>
    public static GaldrTypeInfo<T> Get<T>()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                "GaldrTypeRegistry has not been initialized. Ensure your assembly with [GaldrJsonSerializable] types is loaded.");
        }

        if (!_registry.TryGetValue(typeof(T), out IGaldrTypeInfo info))
        {
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' is not registered. Ensure it has the [GaldrJsonSerializable] attribute.");
        }

        return (GaldrTypeInfo<T>)info;
    }

    /// <summary>
    /// Tries to get the type info for a document type.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="typeInfo">The type info, if found.</param>
    /// <returns>True if the type was found.</returns>
    public static bool TryGet<T>(out GaldrTypeInfo<T> typeInfo)
    {
        bool result = false;
        typeInfo = null;

        if (_registry.TryGetValue(typeof(T), out IGaldrTypeInfo info))
        {
            typeInfo = (GaldrTypeInfo<T>)info;
            result = true;
        }

        return result;
    }

    /// <summary>
    /// Gets the type info for a document type by Type.
    /// </summary>
    /// <param name="type">The document type.</param>
    /// <returns>The type info.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the type is not registered.</exception>
    public static IGaldrTypeInfo Get(Type type)
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                "GaldrTypeRegistry has not been initialized. Ensure your assembly with [GaldrJsonSerializable] types is loaded.");
        }

        if (!_registry.TryGetValue(type, out IGaldrTypeInfo info))
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' is not registered. Ensure it has the [GaldrJsonSerializable] attribute.");
        }

        return info;
    }

    /// <summary>Whether any types have been registered.</summary>
    public static bool IsInitialized
    {
        get { return _initialized; }
    }

    /// <summary>
    /// Gets all registered type infos.
    /// </summary>
    /// <returns>All registered type infos.</returns>
    public static IEnumerable<IGaldrTypeInfo> GetAll()
    {
        return _registry.Values;
    }

    /// <summary>
    /// Checks if a type is a projection type.
    /// </summary>
    /// <typeparam name="T">The type to check.</typeparam>
    /// <returns>True if the type is a projection.</returns>
    public static bool IsProjection<T>()
    {
        bool result = false;

        if (_registry.TryGetValue(typeof(T), out IGaldrTypeInfo info))
        {
            result = info is IGaldrProjectionTypeInfo;
        }

        return result;
    }

    /// <summary>
    /// Checks if a type is a projection type.
    /// </summary>
    /// <param name="type">The type to check.</param>
    /// <returns>True if the type is a projection.</returns>
    public static bool IsProjection(Type type)
    {
        bool result = false;

        if (_registry.TryGetValue(type, out IGaldrTypeInfo info))
        {
            result = info is IGaldrProjectionTypeInfo;
        }

        return result;
    }

    internal static bool Unregister(Type type)
    {
        return _registry.Remove(type);
    }

    internal static void RestoreRegistration(IGaldrTypeInfo typeInfo)
    {
        _registry[typeInfo.DocumentType] = typeInfo;
    }
}
