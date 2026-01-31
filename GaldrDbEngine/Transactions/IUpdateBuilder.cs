using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Query;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Interface for partial update builders. Enables mocking and testability.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public interface IUpdateBuilder<T>
{
    /// <summary>
    /// Sets a field to a new value. Pass null to set the JSON field to null.
    /// </summary>
    /// <typeparam name="TField">The field value type.</typeparam>
    /// <param name="field">The field to set.</param>
    /// <param name="value">The new value.</param>
    /// <returns>This builder for chaining.</returns>
    IUpdateBuilder<T> Set<TField>(GaldrField<T, TField> field, TField value);

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
