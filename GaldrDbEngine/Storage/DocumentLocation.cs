namespace GaldrDbEngine.Storage;

public class DocumentLocation
{
    public int PageId { get; set; }
    public int SlotIndex { get; set; }

    public DocumentLocation(int pageId, int slotIndex)
    {
        PageId = pageId;
        SlotIndex = slotIndex;
    }
}
