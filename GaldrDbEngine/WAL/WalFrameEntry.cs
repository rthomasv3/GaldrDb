namespace GaldrDbEngine.WAL;

internal class WalFrameEntry
{
    public int PageId { get; set; }
    public byte[] Data { get; set; }
}
