using System;
using GaldrDbEngine;

namespace GaldrDbAspNetCore;

/// <summary>
/// Factory for creating and managing named GaldrDb instances.
/// </summary>
public interface IGaldrDbFactory : IDisposable
{
    /// <summary>
    /// Gets a database instance by name.
    /// </summary>
    /// <param name="name">The name of the database instance.</param>
    /// <returns>The database instance.</returns>
    IGaldrDb Get(string name);
}
