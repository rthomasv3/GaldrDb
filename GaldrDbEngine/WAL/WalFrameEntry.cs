namespace GaldrDbEngine.WAL;

internal struct WalFrameEntry
{
    public int PageId;
    public byte PageType;
    public byte[] Data;
}
