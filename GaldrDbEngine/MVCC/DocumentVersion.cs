using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.MVCC;

internal class DocumentVersion
{
    public int DocumentId { get; }
    public TxId CreatedBy { get; }
    public ulong CommitCSN { get; }
    public ulong DeletedCSN { get; private set; }
    public DocumentLocation Location { get; }
    public DocumentVersion PreviousVersion { get; private set; }

    public DocumentVersion(int documentId, TxId createdBy, ulong commitCSN, DocumentLocation location, DocumentVersion previousVersion)
    {
        DocumentId = documentId;
        CreatedBy = createdBy;
        CommitCSN = commitCSN;
        DeletedCSN = ulong.MaxValue;
        Location = location;
        PreviousVersion = previousVersion;
    }

    public void MarkDeleted(ulong deletedCSN)
    {
        DeletedCSN = deletedCSN;
    }

    public bool IsDeleted
    {
        get { return DeletedCSN != ulong.MaxValue; }
    }

    public bool IsVisibleTo(ulong snapshotCSN)
    {
        bool createdBeforeOrAtSnapshot = CommitCSN <= snapshotCSN;
        bool notDeletedOrDeletedAfterSnapshot = DeletedCSN == ulong.MaxValue || DeletedCSN > snapshotCSN;

        return createdBeforeOrAtSnapshot && notDeletedOrDeletedAfterSnapshot;
    }

    internal void SetPreviousVersion(DocumentVersion previousVersion)
    {
        PreviousVersion = previousVersion;
    }
}
