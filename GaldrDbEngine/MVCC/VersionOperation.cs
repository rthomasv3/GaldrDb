using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

internal readonly struct VersionOperation
{
    public string CollectionName { get; }
    public int DocumentId { get; }
    public DocumentLocation Location { get; }
    public bool IsDelete { get; }
    public TxId? ReadVersionTxId { get; }

    public VersionOperation(string collectionName, int documentId, DocumentLocation location, bool isDelete, TxId? readVersionTxId)
    {
        CollectionName = collectionName;
        DocumentId = documentId;
        Location = location;
        IsDelete = isDelete;
        ReadVersionTxId = readVersionTxId;
    }

    public static VersionOperation ForInsert(string collectionName, int documentId, DocumentLocation location)
    {
        return new VersionOperation(collectionName, documentId, location, false, null);
    }

    public static VersionOperation ForUpdate(string collectionName, int documentId, DocumentLocation location, TxId? readVersionTxId)
    {
        return new VersionOperation(collectionName, documentId, location, false, readVersionTxId);
    }

    public static VersionOperation ForDelete(string collectionName, int documentId, TxId? readVersionTxId)
    {
        return new VersionOperation(collectionName, documentId, default, true, readVersionTxId);
    }
}
