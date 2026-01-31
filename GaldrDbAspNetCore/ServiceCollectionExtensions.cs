using System;
using GaldrDbEngine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace GaldrDbAspNetCore;

/// <summary>
/// Extension methods for configuring GaldrDb services in an ASP.NET Core application.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// The default instance name used when no name is specified.
    /// </summary>
    public const string DEFAULT_INSTANCE_NAME = "Default";

    /// <summary>
    /// Adds GaldrDb services to the service collection with the specified options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Action to configure the database options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGaldrDb(
        this IServiceCollection services,
        Action<GaldrDbServiceOptions> configureOptions)
    {
        return services.AddGaldrDb(DEFAULT_INSTANCE_NAME, configureOptions);
    }

    /// <summary>
    /// Adds a named GaldrDb instance to the service collection with the specified options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="name">The name of the database instance.</param>
    /// <param name="configureOptions">Action to configure the database options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddGaldrDb(
        this IServiceCollection services,
        string name,
        Action<GaldrDbServiceOptions> configureOptions)
    {
        services.Configure(name, configureOptions);
        services.TryAddSingleton<IGaldrDbFactory, GaldrDbFactory>();

        if (name == DEFAULT_INSTANCE_NAME)
        {
            services.TryAddSingleton<IGaldrDb>(sp =>
            {
                IGaldrDbFactory factory = sp.GetRequiredService<IGaldrDbFactory>();
                return factory.Get(DEFAULT_INSTANCE_NAME);
            });
        }

        return services;
    }
}
