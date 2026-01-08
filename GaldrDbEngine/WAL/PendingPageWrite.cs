namespace GaldrDbEngine.WAL;

internal sealed class PendingPageWrite
{
    public int PageId { get; }
    public byte[] Data { get; }
    public byte PageType { get; }

    public PendingPageWrite(int pageId, byte[] data, byte pageType)
    {
        PageId = pageId;
        Data = data;
        PageType = pageType;
    }
}
