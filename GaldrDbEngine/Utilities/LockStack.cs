using System;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Utilities;

/// <summary>
/// A stack-like structure for tracking held page locks during B-tree traversal.
/// Supports releasing all locks at once and ordered lock acquisition.
/// This is a class (not struct) to support passing by reference to async methods.
/// </summary>
internal class LockStack
{
    private const int MAX_DEPTH = 32;

    private readonly int[] _pageIds;
    private int _count;
    private readonly PageLockManager _lockManager;
    private readonly bool _isWriteLock;

    public LockStack(PageLockManager lockManager, bool isWriteLock)
    {
        _pageIds = new int[MAX_DEPTH];
        _count = 0;
        _lockManager = lockManager;
        _isWriteLock = isWriteLock;
    }

    public int Count => _count;

    /// <summary>
    /// Push a page ID onto the stack. The lock must already be held.
    /// </summary>
    public void Push(int pageId)
    {
        if (_count >= MAX_DEPTH)
        {
            throw new InvalidOperationException("LockStack overflow - tree depth exceeds maximum");
        }
        _pageIds[_count++] = pageId;
    }

    /// <summary>
    /// Release all held locks and clear the stack.
    /// </summary>
    public void ReleaseAll()
    {
        for (int i = 0; i < _count; i++)
        {
            if (_isWriteLock)
            {
                _lockManager.ReleaseWriteLock(_pageIds[i]);
            }
            else
            {
                _lockManager.ReleaseReadLock(_pageIds[i]);
            }
        }
        _count = 0;
    }

    /// <summary>
    /// Release all held locks except the specified page.
    /// </summary>
    public void ReleaseAllExcept(int keepPageId)
    {
        int writeIndex = 0;
        for (int i = 0; i < _count; i++)
        {
            if (_pageIds[i] == keepPageId)
            {
                _pageIds[writeIndex++] = _pageIds[i];
            }
            else
            {
                if (_isWriteLock)
                {
                    _lockManager.ReleaseWriteLock(_pageIds[i]);
                }
                else
                {
                    _lockManager.ReleaseReadLock(_pageIds[i]);
                }
            }
        }
        _count = writeIndex;
    }

    /// <summary>
    /// Check if a page ID is in the stack.
    /// </summary>
    public bool Contains(int pageId)
    {
        bool found = false;
        for (int i = 0; i < _count && !found; i++)
        {
            if (_pageIds[i] == pageId)
            {
                found = true;
            }
        }
        return found;
    }

    /// <summary>
    /// Get the minimum page ID in the stack, or int.MaxValue if empty.
    /// </summary>
    public int GetMinPageId()
    {
        int min = int.MaxValue;
        for (int i = 0; i < _count; i++)
        {
            if (_pageIds[i] < min)
            {
                min = _pageIds[i];
            }
        }
        return min;
    }

    /// <summary>
    /// Copy current page IDs to an array for re-acquisition in sorted order.
    /// Returns the count of page IDs copied.
    /// </summary>
    public int CopyTo(int[] destination)
    {
        for (int i = 0; i < _count; i++)
        {
            destination[i] = _pageIds[i];
        }
        return _count;
    }

    /// <summary>
    /// Clear the stack without releasing locks (use when locks are being transferred).
    /// </summary>
    public void Clear()
    {
        _count = 0;
    }
}
