using System.Collections.Generic;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Transactions;

internal class WriteSetEntry
{
    public WriteOperation Operation { get; set; }
    public string CollectionName { get; set; }
    public int DocumentId { get; set; }
    public byte[] SerializedData { get; set; }
    public DocumentLocation? PreviousLocation { get; set; }
    public DocumentLocation? NewLocation { get; set; }
    public IReadOnlyList<IndexFieldEntry> IndexFields { get; set; }
    public IReadOnlyList<IndexFieldEntry> OldIndexFields { get; set; }
    public TxId? ReadVersionTxId { get; set; }
}
