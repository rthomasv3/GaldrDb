using System;
using System.Threading;

namespace GaldrDbEngine.IO;

internal class LruCacheEntry
{
    public readonly int PageId;
    public readonly byte[] Data;
    public readonly object WriteLock;
    public long LastAccessedTicks;
    public int Version;

    public LruCacheEntry(int pageId, int pageSize)
    {
        PageId = pageId;
        Data = new byte[pageSize];
        WriteLock = new object();
        LastAccessedTicks = Environment.TickCount64;
        Version = 0;
    }

    public void Touch()
    {
        Interlocked.Exchange(ref LastAccessedTicks, Environment.TickCount64);
    }

    public void BeginWrite()
    {
        Interlocked.Increment(ref Version);
    }

    public void EndWrite()
    {
        Interlocked.Increment(ref Version);
    }

    public int ReadVersion()
    {
        return Volatile.Read(ref Version);
    }

    public bool IsWriteInProgress(int version)
    {
        return (version & 1) == 1;
    }
}
