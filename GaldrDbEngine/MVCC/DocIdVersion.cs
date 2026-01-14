namespace GaldrDbEngine.MVCC;

public sealed class DocIdVersion
{
    public int DocId { get; }
    public DocumentVersion Version { get; }

    public DocIdVersion(int docId, DocumentVersion version)
    {
        DocId = docId;
        Version = version;
    }
}
