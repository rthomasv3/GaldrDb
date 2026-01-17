namespace GaldrDbEngine.Transactions;

/// <summary>
/// Represents the current state of a transaction.
/// </summary>
public enum TransactionState
{
    /// <summary>
    /// The transaction is active and can accept operations.
    /// </summary>
    Active,

    /// <summary>
    /// The transaction is in the process of committing.
    /// </summary>
    Committing,

    /// <summary>
    /// The transaction has been successfully committed.
    /// </summary>
    Committed,

    /// <summary>
    /// The transaction has been aborted or rolled back.
    /// </summary>
    Aborted
}
