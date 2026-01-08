namespace GaldrDbEngine.Transactions;

public enum TransactionState
{
    Active,
    Committing,
    Committed,
    Aborted
}
