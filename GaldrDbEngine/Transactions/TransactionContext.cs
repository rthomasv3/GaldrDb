using System.Collections.Generic;
using System.Collections.Immutable;
using GaldrDbEngine.WAL;

namespace GaldrDbEngine.Transactions;

/// <summary>
/// Holds per-transaction WAL state. Passed explicitly through the call stack
/// to replace AsyncLocal-based ambient state.
/// </summary>
internal sealed class TransactionContext
{
    /// <summary>
    /// The transaction ID for this context.
    /// </summary>
    public ulong TxId { get; init; }

    /// <summary>
    /// The snapshot transaction ID for conflict detection.
    /// </summary>
    public ulong SnapshotTxId { get; init; }

    /// <summary>
    /// The snapshot CSN (Commit Sequence Number) at transaction start.
    /// Used for MVCC visibility instead of TxId comparison.
    /// </summary>
    public ulong SnapshotCSN { get; init; }

    /// <summary>
    /// Snapshot of _pageLatestFrame at transaction start for consistent reads.
    /// Can be refreshed on commit retry after PageConflictException.
    /// </summary>
    public ImmutableDictionary<int, long> FrameSnapshot { get; set; }

    /// <summary>
    /// The _mxFrame value at the time the snapshot was captured.
    /// Used to remove the correct entry from _activeSnapshotFrames on commit/abort.
    /// Can be refreshed on commit retry after PageConflictException.
    /// </summary>
    public long SnapshotMxFrame { get; set; }

    /// <summary>
    /// Uncommitted page writes for this transaction.
    /// Key: pageId, Value: PageWriteEntry containing frame number and base frame.
    /// Set when BeginWrite is called.
    /// </summary>
    public Dictionary<int, PageWriteEntry> PageWrites { get; set; }
}
