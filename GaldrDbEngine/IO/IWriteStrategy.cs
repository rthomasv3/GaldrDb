using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.IO;

internal interface IWriteStrategy : IPageIO
{
    TransactionContext BeginSnapshot(ulong txId, ulong snapshotTxId, ulong snapshotCSN);
    void EndSnapshot(TransactionContext context);
    void RefreshSnapshot(TransactionContext context);
    void BeginWrite(TransactionContext context);
    void CommitWrite(TransactionContext context);
    void AbortWrite(TransactionContext context);
    bool Checkpoint();
    Task<bool> CheckpointAsync(CancellationToken cancellationToken = default);
}
