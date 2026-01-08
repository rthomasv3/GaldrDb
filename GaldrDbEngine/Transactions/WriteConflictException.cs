using System;

namespace GaldrDbEngine.Transactions;

public class WriteConflictException : Exception
{
    public string CollectionName { get; }
    public int DocumentId { get; }
    public TxId ConflictingTxId { get; }

    public WriteConflictException(string message)
        : base(message)
    {
    }

    public WriteConflictException(string message, string collectionName, int documentId, TxId conflictingTxId)
        : base(message)
    {
        CollectionName = collectionName;
        DocumentId = documentId;
        ConflictingTxId = conflictingTxId;
    }

    public WriteConflictException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
