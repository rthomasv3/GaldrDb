namespace GaldrDbEngine.Storage;

public struct DeletePathEntry
{
    public int PageId;
    public int ChildIndex;

    public DeletePathEntry(int pageId, int childIndex)
    {
        PageId = pageId;
        ChildIndex = childIndex;
    }
}
