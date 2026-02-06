using System;

namespace GaldrDbEngine.Transactions;

internal sealed class PageConflictException : Exception
{
    public int PageId { get; }
    public long BaseVersion { get; }
    public long CurrentVersion { get; }

    public PageConflictException(int pageId, long baseVersion, long currentVersion)
        : base($"Page conflict detected: page {pageId} was modified by another transaction (base={baseVersion}, current={currentVersion})")
    {
        PageId = pageId;
        BaseVersion = baseVersion;
        CurrentVersion = currentVersion;
    }
}
