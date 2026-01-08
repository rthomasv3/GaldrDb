using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

public class DocumentVersion
{
    public TxId CreatedBy { get; }
    public TxId DeletedBy { get; private set; }
    public DocumentLocation Location { get; }
    public DocumentVersion PreviousVersion { get; private set; }

    public DocumentVersion(TxId createdBy, DocumentLocation location, DocumentVersion previousVersion)
    {
        CreatedBy = createdBy;
        DeletedBy = TxId.MaxValue;
        Location = location;
        PreviousVersion = previousVersion;
    }

    public void MarkDeleted(TxId deletedBy)
    {
        DeletedBy = deletedBy;
    }

    public bool IsDeleted
    {
        get { return DeletedBy != TxId.MaxValue; }
    }

    public bool IsVisibleTo(TxId snapshotTxId)
    {
        bool createdBeforeOrAtSnapshot = CreatedBy <= snapshotTxId;
        bool notDeletedOrDeletedAfterSnapshot = DeletedBy == TxId.MaxValue || DeletedBy > snapshotTxId;

        return createdBeforeOrAtSnapshot && notDeletedOrDeletedAfterSnapshot;
    }

    internal void SetPreviousVersion(DocumentVersion previousVersion)
    {
        PreviousVersion = previousVersion;
    }
}
