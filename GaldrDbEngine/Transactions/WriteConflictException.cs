using System;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Exception thrown when a write conflict is detected during transaction commit.
/// </summary>
public class WriteConflictException : Exception
{
    /// <summary>
    /// The collection where the conflict occurred.
    /// </summary>
    public string CollectionName { get; }

    /// <summary>
    /// The document ID that caused the conflict.
    /// </summary>
    public int DocumentId { get; }

    /// <summary>
    /// The transaction ID of the conflicting transaction.
    /// </summary>
    public TxId ConflictingTxId { get; }

    /// <summary>
    /// Creates a new WriteConflictException with the specified message.
    /// </summary>
    /// <param name="message">The error message.</param>
    internal WriteConflictException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates a new WriteConflictException with conflict details.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="collectionName">The collection where the conflict occurred.</param>
    /// <param name="documentId">The document ID that caused the conflict.</param>
    /// <param name="conflictingTxId">The transaction ID of the conflicting transaction.</param>
    internal WriteConflictException(string message, string collectionName, int documentId, TxId conflictingTxId)
        : base(message)
    {
        CollectionName = collectionName;
        DocumentId = documentId;
        ConflictingTxId = conflictingTxId;
    }

    /// <summary>
    /// Creates a new WriteConflictException with the specified message and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal WriteConflictException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
