using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using GaldrDbEngine;
using Microsoft.Extensions.Options;

namespace GaldrDbAspNetCore;

/// <summary>
/// Factory for creating and managing named GaldrDb instances.
/// Uses ConcurrentDictionary with Lazy for thread-safe, lock-free access in web API scenarios.
/// </summary>
internal sealed class GaldrDbFactory : IGaldrDbFactory
{
    private readonly IOptionsMonitor<GaldrDbServiceOptions> _optionsMonitor;
    private readonly ConcurrentDictionary<string, Lazy<IGaldrDb>> _instances;
    private volatile bool _disposed;

    public GaldrDbFactory(IOptionsMonitor<GaldrDbServiceOptions> optionsMonitor)
    {
        _optionsMonitor = optionsMonitor;
        _instances = new ConcurrentDictionary<string, Lazy<IGaldrDb>>(StringComparer.OrdinalIgnoreCase);
    }

    public IGaldrDb Get(string name)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(GaldrDbFactory));
        }

        Lazy<IGaldrDb> lazy = _instances.GetOrAdd(
            name,
            key => new Lazy<IGaldrDb>(() => CreateInstance(key)));

        return lazy.Value;
    }

    private IGaldrDb CreateInstance(string name)
    {
        GaldrDbServiceOptions options = _optionsMonitor.Get(name);

        if (string.IsNullOrEmpty(options.FilePath))
        {
            throw new InvalidOperationException($"FilePath is not configured for GaldrDb instance '{name}'.");
        }

        GaldrDb db;

        switch (options.OpenMode)
        {
            case GaldrDbOpenMode.Open:
                db = GaldrDb.Open(options.FilePath, options.DatabaseOptions);
                break;
            case GaldrDbOpenMode.Create:
                db = GaldrDb.Create(options.FilePath, options.DatabaseOptions ?? new GaldrDbOptions());
                break;
            case GaldrDbOpenMode.OpenOrCreate:
            default:
                db = GaldrDb.OpenOrCreate(options.FilePath, options.DatabaseOptions);
                break;
        }

        return db;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;

            foreach (KeyValuePair<string, Lazy<IGaldrDb>> kvp in _instances)
            {
                if (kvp.Value.IsValueCreated)
                {
                    kvp.Value.Value.Dispose();
                }
            }

            _instances.Clear();
        }
    }
}
