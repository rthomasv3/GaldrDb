namespace GaldrDbEngine.Pages;

public static class PageConstants
{
    public const uint MAGIC_NUMBER = 0x47414C44;
    public const int VERSION = 1;
    public const byte PAGE_TYPE_DOCUMENT = 1;
    public const byte PAGE_TYPE_BTREE = 2;
    public const byte PAGE_TYPE_SECONDARY_INDEX = 3;

    // Pages 0-3 are reserved: Header (0), Bitmap (1), FSM (2), Collections Metadata (3)
    public const int FIRST_DATA_PAGE_ID = 4;
}
