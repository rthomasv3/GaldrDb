using System;

namespace GaldrDbEngine.WAL;

internal sealed class PageConflictException : Exception
{
    public int PageId { get; }
    public long BaseFrame { get; }
    public long CurrentFrame { get; }

    public PageConflictException(int pageId, long baseFrame, long currentFrame)
        : base($"Page conflict detected: page {pageId} was modified by another transaction (base={baseFrame}, current={currentFrame})")
    {
        PageId = pageId;
        BaseFrame = baseFrame;
        CurrentFrame = currentFrame;
    }
}
