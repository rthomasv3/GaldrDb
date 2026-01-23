namespace GaldrDbEngine.Storage;

internal readonly struct SecondaryIndexEntry
{
    public int DocId { get; }
    public DocumentLocation Location { get; }

    public SecondaryIndexEntry(int docId, DocumentLocation location)
    {
        DocId = docId;
        Location = location;
    }
}
