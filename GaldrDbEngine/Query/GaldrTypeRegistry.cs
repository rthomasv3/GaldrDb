using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Query;

public static class GaldrTypeRegistry
{
    private static readonly Dictionary<Type, IGaldrTypeInfo> _registry = new Dictionary<Type, IGaldrTypeInfo>();
    private static bool _initialized;

    public static void Register(IGaldrTypeInfo typeInfo)
    {
        _registry[typeInfo.DocumentType] = typeInfo;
        _initialized = true;
    }

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

    public static bool IsInitialized
    {
        get { return _initialized; }
    }

    public static IEnumerable<IGaldrTypeInfo> GetAll()
    {
        return _registry.Values;
    }

    public static bool IsProjection<T>()
    {
        bool result = false;

        if (_registry.TryGetValue(typeof(T), out IGaldrTypeInfo info))
        {
            result = info is IGaldrProjectionTypeInfo;
        }

        return result;
    }

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
