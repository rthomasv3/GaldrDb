using System.Collections.Generic;

namespace GaldrDbEngine.IO;

internal class LruCacheEntry
{
    public int PageId;
    public byte[] Data;
    public LinkedListNode<LruCacheEntry> Node;
}
