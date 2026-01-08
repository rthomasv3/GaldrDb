namespace GaldrDbEngine.MVCC;

public sealed class GarbageCollectionResult
{
    public int VersionsCollected { get; }
    public int DocumentsProcessed { get; }

    public GarbageCollectionResult(int versionsCollected, int documentsProcessed)
    {
        VersionsCollected = versionsCollected;
        DocumentsProcessed = documentsProcessed;
    }
}
