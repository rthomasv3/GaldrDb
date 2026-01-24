namespace GaldrDbEngine.Pages;

internal struct ReadDocumentResult
{
    public byte[] DocumentData { get; set; }
    public SlotEntry Slot { get; set; }
}
