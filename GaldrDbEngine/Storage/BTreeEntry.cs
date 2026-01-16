namespace GaldrDbEngine.Storage;

internal class BTreeEntry
{
    public int Key { get; }
    public DocumentLocation Location { get; }

    public BTreeEntry(int key, DocumentLocation location)
    {
        Key = key;
        Location = location;
    }
}
