using System;

namespace GaldrDbEngine.Pages;

/// <summary>
/// Exception thrown when attempting to read a document slot that has been deleted by garbage collection.
/// This indicates a write conflict - the document was modified/deleted by another committed transaction.
/// </summary>
internal sealed class DocumentSlotDeletedException : Exception
{
    public int PageId { get; }
    public int SlotIndex { get; }

    public DocumentSlotDeletedException(int pageId, int slotIndex)
        : base($"Document slot has been deleted: page {pageId}, slot {slotIndex}")
    {
        PageId = pageId;
        SlotIndex = slotIndex;
    }
}
