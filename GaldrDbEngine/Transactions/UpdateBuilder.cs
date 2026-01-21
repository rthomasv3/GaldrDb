using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Query;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Fluent builder for performing partial updates on a document by ID.
/// Allows updating individual fields without loading the full typed document.
/// </summary>
/// <typeparam name="T">The document type.</typeparam>
public sealed class UpdateBuilder<T>
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly GaldrTypeInfo<T> _typeInfo;
    private readonly int _documentId;
    private readonly List<FieldModification> _modifications;

    /// <summary>
    /// Creates an UpdateBuilder backed by a transaction.
    /// </summary>
    internal UpdateBuilder(Transaction transaction, GaldrTypeInfo<T> typeInfo, int documentId)
    {
        _transaction = transaction;
        _db = null;
        _typeInfo = typeInfo;
        _documentId = documentId;
        _modifications = new List<FieldModification>();
    }

    /// <summary>
    /// Creates an UpdateBuilder backed by a GaldrDb (auto-transaction on execute).
    /// </summary>
    internal UpdateBuilder(GaldrDb db, GaldrTypeInfo<T> typeInfo, int documentId)
    {
        _transaction = null;
        _db = db;
        _typeInfo = typeInfo;
        _documentId = documentId;
        _modifications = new List<FieldModification>();
    }

    /// <summary>
    /// Sets a field to a new value. Pass null to set the JSON field to null.
    /// </summary>
    /// <typeparam name="TField">The field value type.</typeparam>
    /// <param name="field">The field to set.</param>
    /// <param name="value">The new value.</param>
    /// <returns>This builder for chaining.</returns>
    public UpdateBuilder<T> Set<TField>(GaldrField<T, TField> field, TField value)
    {
        FieldModification mod = new FieldModification
        {
            FieldName = field.FieldName,
            FieldType = field.FieldType,
            Value = value
        };
        _modifications.Add(mod);
        return this;
    }

    /// <summary>
    /// Executes the partial update.
    /// </summary>
    /// <returns>True if the document was found and updated, false if not found.</returns>
    public bool Execute()
    {
        bool result;

        if (_transaction != null)
        {
            result = _transaction.ExecutePartialUpdate(_typeInfo, _documentId, _modifications);
        }
        else
        {
            using (Transaction tx = _db.BeginTransaction())
            {
                result = tx.ExecutePartialUpdate(_typeInfo, _documentId, _modifications);
                if (result)
                {
                    tx.Commit();
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Executes the partial update asynchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false if not found.</returns>
    public async Task<bool> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        bool result;

        if (_transaction != null)
        {
            result = await _transaction.ExecutePartialUpdateAsync(_typeInfo, _documentId, _modifications, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            using (Transaction tx = _db.BeginTransaction())
            {
                result = await tx.ExecutePartialUpdateAsync(_typeInfo, _documentId, _modifications, cancellationToken).ConfigureAwait(false);
                if (result)
                {
                    await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        return result;
    }
}
