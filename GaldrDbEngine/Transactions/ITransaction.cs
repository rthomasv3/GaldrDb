using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.Query;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Interface for database transactions. Enables mocking and testability.
/// </summary>
public interface ITransaction : IDisposable
{
    #region Properties

    /// <summary>
    /// The unique transaction identifier.
    /// </summary>
    TxId TxId { get; }

    /// <summary>
    /// The snapshot transaction ID that determines visibility of data.
    /// </summary>
    TxId SnapshotTxId { get; }

    /// <summary>
    /// The current state of the transaction.
    /// </summary>
    TransactionState State { get; }

    /// <summary>
    /// Whether this is a read-only transaction.
    /// </summary>
    bool IsReadOnly { get; }

    /// <summary>
    /// Number of pending writes in this transaction.
    /// </summary>
    int WriteSetCount { get; }

    #endregion

    #region Query

    /// <summary>
    /// Creates a query builder for the specified document type within this transaction.
    /// </summary>
    /// <typeparam name="T">The document type to query.</typeparam>
    /// <returns>A query builder for constructing and executing queries.</returns>
    QueryBuilder<T> Query<T>();

    /// <summary>
    /// Creates a dynamic query builder for the specified collection within this transaction.
    /// </summary>
    /// <param name="collectionName">The collection name to query.</param>
    /// <returns>A dynamic query builder for constructing and executing queries.</returns>
    DynamicQueryBuilder QueryDynamic(string collectionName);

    #endregion

    #region Type-Safe CRUD Operations

    /// <summary>
    /// Gets a document by its ID within this transaction's snapshot.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>The document, or default if not found.</returns>
    T GetById<T>(int id);

    /// <summary>
    /// Asynchronously gets a document by its ID within this transaction's snapshot.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or default if not found.</returns>
    Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    int Insert<T>(T document);

    /// <summary>
    /// Asynchronously inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
    Task<int> InsertAsync<T>(T document, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    bool Replace<T>(T document);

    /// <summary>
    /// Asynchronously updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    Task<bool> ReplaceAsync<T>(T document, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a partial update builder for updating specific fields of a document by ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>An UpdateBuilder for chaining Set calls.</returns>
    IUpdateBuilder<T> UpdateById<T>(int id);

    /// <summary>
    /// Deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    bool DeleteById<T>(int id);

    /// <summary>
    /// Asynchronously deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    Task<bool> DeleteByIdAsync<T>(int id, CancellationToken cancellationToken = default);

    #endregion

    #region Dynamic CRUD Operations

    /// <summary>
    /// Gets a document by ID as a JsonDocument within this transaction's snapshot.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    JsonDocument GetByIdDynamic(string collectionName, int id);

    /// <summary>
    /// Asynchronously gets a document by ID as a JsonDocument within this transaction's snapshot.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    Task<JsonDocument> GetByIdDynamicAsync(string collectionName, int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts a JSON document and returns its assigned ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="json">The JSON document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    int InsertDynamic(string collectionName, string json);

    /// <summary>
    /// Asynchronously inserts a JSON document and returns its assigned ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="json">The JSON document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
    Task<int> InsertDynamicAsync(string collectionName, string json, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces an existing JSON document.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="json">The JSON document with updated values.</param>
    /// <returns>True if the document was found and replaced, false otherwise.</returns>
    bool ReplaceDynamic(string collectionName, int id, string json);

    /// <summary>
    /// Asynchronously replaces an existing JSON document.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="json">The JSON document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and replaced, false otherwise.</returns>
    Task<bool> ReplaceDynamicAsync(string collectionName, int id, string json, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a builder for performing a partial update on a document by ID using runtime field names.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>A DynamicUpdateBuilder for chaining Set calls.</returns>
    IDynamicUpdateBuilder UpdateByIdDynamic(string collectionName, int id);

    /// <summary>
    /// Deletes a document by its ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    bool DeleteByIdDynamic(string collectionName, int id);

    /// <summary>
    /// Asynchronously deletes a document by its ID.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    Task<bool> DeleteByIdDynamicAsync(string collectionName, int id, CancellationToken cancellationToken = default);

    #endregion

    #region Transaction Control

    /// <summary>
    /// Commits all pending changes in this transaction.
    /// </summary>
    void Commit();

    /// <summary>
    /// Asynchronously commits all pending changes in this transaction.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back all pending changes and aborts this transaction.
    /// </summary>
    void Rollback();

    #endregion
}
