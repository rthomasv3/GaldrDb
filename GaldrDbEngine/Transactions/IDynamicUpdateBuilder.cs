using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Interface for dynamic partial update builders. Enables mocking and testability.
/// </summary>
public interface IDynamicUpdateBuilder
{
    /// <summary>
    /// Sets a field to a new value. The type is inferred from the value.
    /// Supported types: string, int, long, double, decimal, bool, DateTime, DateTimeOffset, Guid.
    /// Pass null to set the JSON field to null.
    /// </summary>
    /// <param name="fieldName">The field name to set.</param>
    /// <param name="value">The new value.</param>
    /// <returns>This builder for chaining.</returns>
    IDynamicUpdateBuilder Set(string fieldName, object value);

    /// <summary>
    /// Executes the partial update.
    /// </summary>
    /// <returns>True if the document was found and updated, false if not found.</returns>
    bool Execute();

    /// <summary>
    /// Executes the partial update asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false if not found.</returns>
    Task<bool> ExecuteAsync(CancellationToken cancellationToken = default);
}
