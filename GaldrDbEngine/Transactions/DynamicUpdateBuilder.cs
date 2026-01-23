using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Query;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Fluent builder for performing partial updates on a document by ID using runtime field names.
/// Allows updating individual fields without loading the full typed document.
/// </summary>
public sealed class DynamicUpdateBuilder
{
    private readonly Transaction _transaction;
    private readonly GaldrDb _db;
    private readonly string _collectionName;
    private readonly int _documentId;
    private readonly List<FieldModification> _modifications;

    /// <summary>
    /// Creates a DynamicUpdateBuilder backed by a transaction.
    /// </summary>
    internal DynamicUpdateBuilder(Transaction transaction, string collectionName, int documentId)
    {
        _transaction = transaction;
        _db = null;
        _collectionName = collectionName;
        _documentId = documentId;
        _modifications = new List<FieldModification>();
    }

    /// <summary>
    /// Creates a DynamicUpdateBuilder backed by a GaldrDb (auto-transaction on execute).
    /// </summary>
    internal DynamicUpdateBuilder(GaldrDb db, string collectionName, int documentId)
    {
        _transaction = null;
        _db = db;
        _collectionName = collectionName;
        _documentId = documentId;
        _modifications = new List<FieldModification>();
    }

    /// <summary>
    /// Sets a field to a new value. The type is inferred from the value.
    /// Supported types: string, int, long, double, decimal, bool, DateTime, DateTimeOffset, Guid.
    /// Pass null to set the JSON field to null.
    /// </summary>
    /// <param name="fieldName">The field name to set.</param>
    /// <param name="value">The new value.</param>
    /// <returns>This builder for chaining.</returns>
    public DynamicUpdateBuilder Set(string fieldName, object value)
    {
        GaldrFieldType fieldType = InferFieldType(value);

        _modifications.Add(new FieldModification
        {
            FieldName = fieldName,
            FieldType = fieldType,
            Value = value
        });

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
            result = _transaction.ExecutePartialUpdateDynamic(_collectionName, _documentId, _modifications);
        }
        else
        {
            using (Transaction tx = _db.BeginTransaction())
            {
                result = tx.ExecutePartialUpdateDynamic(_collectionName, _documentId, _modifications);
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
            result = await _transaction.ExecutePartialUpdateDynamicAsync(_collectionName, _documentId, _modifications, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            using (Transaction tx = _db.BeginTransaction())
            {
                result = await tx.ExecutePartialUpdateDynamicAsync(_collectionName, _documentId, _modifications, cancellationToken).ConfigureAwait(false);
                if (result)
                {
                    await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        return result;
    }

    private static GaldrFieldType InferFieldType(object value)
    {
        return value switch
        {
            string => GaldrFieldType.String,
            int => GaldrFieldType.Int32,
            long => GaldrFieldType.Int64,
            double => GaldrFieldType.Double,
            decimal => GaldrFieldType.Decimal,
            bool => GaldrFieldType.Boolean,
            DateTime => GaldrFieldType.DateTime,
            DateTimeOffset => GaldrFieldType.DateTimeOffset,
            Guid => GaldrFieldType.Guid,
            null => GaldrFieldType.String,
            _ => throw new NotSupportedException(
                $"Partial update for value type '{value.GetType().Name}' is not supported. " +
                "Supported types: string, int, long, double, decimal, bool, DateTime, DateTimeOffset, Guid.")
        };
    }
}
