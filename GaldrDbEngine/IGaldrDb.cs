using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Json;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Schema;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine;

/// <summary>
/// Interface for GaldrDb database operations. Enables mocking and testability.
/// </summary>
public interface IGaldrDb : IDisposable
{
    #region Lifecycle

    /// <summary>
    /// Checkpoints the WAL, applying all pending writes to the main database file.
    /// </summary>
    void Checkpoint();

    /// <summary>
    /// Asynchronously checkpoints the WAL, applying all pending writes to the main database file.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CheckpointAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes old document versions and compacts fragmented pages.
    /// </summary>
    /// <returns>Statistics about the vacuum operation.</returns>
    GarbageCollectionResult Vacuum();

    /// <summary>
    /// Asynchronously removes old document versions and compacts fragmented pages.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Statistics about the vacuum operation.</returns>
    Task<GarbageCollectionResult> VacuumAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a compacted copy of the database at the target path.
    /// </summary>
    /// <param name="targetPath">Path for the compacted database file.</param>
    /// <returns>Statistics about the compaction.</returns>
    DatabaseCompactResult CompactTo(string targetPath);

    /// <summary>
    /// Asynchronously creates a compacted copy of the database at the target path.
    /// </summary>
    /// <param name="targetPath">Path for the compacted database file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Statistics about the compaction.</returns>
    Task<DatabaseCompactResult> CompactToAsync(string targetPath, CancellationToken cancellationToken = default);

    #endregion

    #region Transactions

    /// <summary>
    /// Begins a new read-write transaction with snapshot isolation.
    /// </summary>
    /// <returns>A new transaction that must be committed or disposed.</returns>
    ITransaction BeginTransaction();

    /// <summary>
    /// Begins a new read-only transaction with snapshot isolation.
    /// </summary>
    /// <returns>A new read-only transaction that must be disposed when complete.</returns>
    ITransaction BeginReadOnlyTransaction();

    #endregion

    #region Schema Management

    /// <summary>
    /// Gets the names of all collections in the database.
    /// </summary>
    /// <returns>A list of collection names.</returns>
    IReadOnlyList<string> GetCollectionNames();

    /// <summary>
    /// Gets the names of all indexes on a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>A list of index field names.</returns>
    IReadOnlyList<string> GetIndexNames(string collectionName);

    /// <summary>
    /// Gets detailed information about all indexes on a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>A list of index information.</returns>
    IReadOnlyList<IndexInfo> GetIndexes(string collectionName);

    /// <summary>
    /// Gets detailed information about a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <returns>Collection information including document count and indexes.</returns>
    CollectionInfo GetCollectionInfo(string collectionName);

    /// <summary>
    /// Drops an index from a collection.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="fieldName">The indexed field name.</param>
    void DropIndex(string collectionName, string fieldName);

    /// <summary>
    /// Drops a collection and optionally deletes all its documents.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="deleteDocuments">If true, deletes all documents. If false and documents exist, throws.</param>
    void DropCollection(string collectionName, bool deleteDocuments = false);

    /// <summary>
    /// Detects collections and indexes in the database that are not registered with any type.
    /// </summary>
    /// <returns>Information about orphaned collections and indexes.</returns>
    OrphanedSchemaInfo GetOrphanedSchema();

    /// <summary>
    /// Removes orphaned collections and indexes from the database.
    /// </summary>
    /// <param name="deleteDocuments">If true, deletes documents in orphaned collections.</param>
    /// <returns>Information about what was cleaned up.</returns>
    OrphanedSchemaInfo CleanupOrphanedSchema(bool deleteDocuments = false);

    #endregion

    #region Type-Safe CRUD Operations

    /// <summary>
    /// Inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <returns>The assigned document ID.</returns>
    int Insert<T>(T document);

    /// <summary>
    /// Gets a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <returns>The document, or default if not found.</returns>
    T GetById<T>(int id);

    /// <summary>
    /// Updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    bool Replace<T>(T document);

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
    /// Creates a query builder for the specified document type.
    /// </summary>
    /// <typeparam name="T">The document type to query.</typeparam>
    /// <returns>A query builder for constructing and executing queries.</returns>
    QueryBuilder<T> Query<T>();

    #endregion

    #region Async Type-Safe CRUD Operations

    /// <summary>
    /// Asynchronously inserts a document and returns its assigned ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The assigned document ID.</returns>
    Task<int> InsertAsync<T>(T document, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously gets a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The document, or default if not found.</returns>
    Task<T> GetByIdAsync<T>(int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously updates an existing document.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="document">The document with updated values.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and updated, false otherwise.</returns>
    Task<bool> ReplaceAsync<T>(T document, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes a document by its ID.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the document was found and deleted, false otherwise.</returns>
    Task<bool> DeleteAsync<T>(int id, CancellationToken cancellationToken = default);

    #endregion

    #region Dynamic CRUD Operations

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
    /// Gets a document by ID as a JsonDocument.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    JsonDocument GetByIdDynamic(string collectionName, int id);

    /// <summary>
    /// Asynchronously gets a document by ID as a JsonDocument.
    /// </summary>
    /// <param name="collectionName">The collection name.</param>
    /// <param name="id">The document ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The JsonDocument, or null if not found.</returns>
    Task<JsonDocument> GetByIdDynamicAsync(string collectionName, int id, CancellationToken cancellationToken = default);

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

    /// <summary>
    /// Creates a dynamic query builder for the specified collection.
    /// </summary>
    /// <param name="collectionName">The collection name to query.</param>
    /// <returns>A dynamic query builder for constructing and executing queries.</returns>
    DynamicQueryBuilder QueryDynamic(string collectionName);

    #endregion
}
