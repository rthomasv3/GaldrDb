namespace GaldrDbEngine.Storage;

public readonly struct DocumentLocation
{
    public int PageId { get; }
    public int SlotIndex { get; }

    public DocumentLocation(int pageId, int slotIndex)
    {
        PageId = pageId;
        SlotIndex = slotIndex;
    }
}
