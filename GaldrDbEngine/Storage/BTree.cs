using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class BTree
{
    private const int MAX_TREE_DEPTH = 32;

    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly PageLockManager _pageLockManager;
    private readonly AsyncReaderWriterLock _rootLock;
    private readonly int _pageSize;
    private readonly int _order;
    private int _rootPageId;

    public BTree(IPageIO pageIO, PageManager pageManager, PageLockManager pageLockManager, int rootPageId, int pageSize, int order)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _pageLockManager = pageLockManager;
        _rootLock = new AsyncReaderWriterLock();
        _rootPageId = rootPageId;
        _pageSize = pageSize;
        _order = order;
    }

    public int GetRootPageId()
    {
        return _rootPageId;
    }

    public void Insert(int docId, DocumentLocation location)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        BTreeNode newRoot = null;
        LockStack heldLocks = new LockStack(_pageLockManager, true);

        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            _pageIO.ReadPage(rootPageId, buffer);

            if (BTreeNode.IsNodeFull(buffer, _order))
            {
                // Root is full - need to split it first
                BTreeNode root = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
                try
                {
                    BTreeNode.DeserializeTo(buffer, root, _order);

                    int newRootPageId = _pageManager.AllocatePage();
                    _pageLockManager.AcquireWriteLock(newRootPageId);
                    newRoot = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
                    newRoot.ChildPageIds.Add(rootPageId);

                    newRoot.SerializeTo(buffer);
                    _pageIO.WritePage(newRootPageId, buffer);

                    SplitChild(newRootPageId, 0, rootPageId);
                    _rootPageId = newRootPageId;

                    // Release rootPageId lock - InsertNonFull will acquire child locks as needed
                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    // Re-read the new root for InsertNonFull
                    _pageIO.ReadPage(newRootPageId, buffer);
                    InsertNonFull(newRootPageId, buffer, childBuffer, docId, location, heldLocks);
                }
                finally
                {
                    BTreeNodePool.Return(root);
                }
            }
            else
            {
                // Root won't change, keep page lock
                heldLocks.Push(rootPageId);
                InsertNonFull(rootPageId, buffer, childBuffer, docId, location, heldLocks);
            }
        }
        finally
        {
            _rootLock.ExitWriteLock();
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
            BTreeNodePool.Return(newRoot);
        }
    }

    public DocumentLocation? Search(int docId)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireReadLock(rootPageId);
            return SearchNode(rootPageId, docId, rootAlreadyLocked: true);
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Updates the location for an existing key. Returns true if key was found and updated.
    /// This is more efficient than Delete + Insert when only the value changes.
    /// </summary>
    public bool Update(int docId, DocumentLocation newLocation)
    {
        bool updated = false;

        _rootLock.EnterWriteLock();
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            int currentPageId = rootPageId;

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);
                int pos = BTreeNode.FindKeyPosition(buffer, keyCount, docId);

                if (BTreeNode.IsLeafNode(buffer))
                {
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        BTreeNode.SetLeafValue(buffer, keyCount, pos, newLocation);
                        _pageIO.WritePage(currentPageId, buffer);
                        updated = true;
                    }
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, pos);
                    _pageLockManager.AcquireWriteLock(childPageId);
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = childPageId;
                }
            }
        }
        finally
        {
            _rootLock.ExitWriteLock();
            BufferPool.Return(buffer);
        }

        return updated;
    }

    public List<BTreeEntry> SearchRange(int startDocId, int endDocId, bool includeStart, bool includeEnd)
    {
        List<BTreeEntry> results = new List<BTreeEntry>();

        _rootLock.EnterReadLock();
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireReadLock(rootPageId);

            // FindLeafForKey returns with read lock held on the leaf
            int leafPageId = FindLeafForKey(rootPageId, startDocId, buffer, node, rootAlreadyLocked: true);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                // Read lock already held on currentPageId from FindLeafForKey or previous iteration
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                bool exceededEnd = ScanLeafForRange(node, startDocId, endDocId, includeStart, includeEnd, results);

                if (exceededEnd || node.NextLeaf == 0)
                {
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    continueScanning = false;
                }
                else
                {
                    int nextPageId = node.NextLeaf;
                    _pageLockManager.AcquireReadLock(nextPageId);
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = nextPageId;
                }
            }
        }
        finally
        {
            _rootLock.ExitReadLock();
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return results;
    }

    /// <summary>
    /// Finds the leaf containing the target key. Returns with read lock held on the leaf.
    /// </summary>
    private int FindLeafForKey(int pageId, int targetKey, byte[] buffer, BTreeNode node, bool rootAlreadyLocked = false)
    {
        int currentPageId = pageId;
        if (!rootAlreadyLocked)
        {
            _pageLockManager.AcquireReadLock(currentPageId);
        }

        while (currentPageId != 0)
        {
            _pageIO.ReadPage(currentPageId, buffer);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                // Return with read lock still held
                break;
            }

            int i = 0;
            while (i < node.KeyCount && targetKey > node.Keys[i])
            {
                i++;
            }

            int childPageId = node.ChildPageIds[i];
            _pageLockManager.AcquireReadLock(childPageId);
            _pageLockManager.ReleaseReadLock(currentPageId);
            currentPageId = childPageId;
        }

        return currentPageId;
    }

    private bool ScanLeafForRange(BTreeNode leaf, int startDocId, int endDocId, bool includeStart, bool includeEnd, List<BTreeEntry> results)
    {
        bool exceededEnd = false;

        for (int i = 0; i < leaf.KeyCount && !exceededEnd; i++)
        {
            int key = leaf.Keys[i];

            if (key > endDocId || (key == endDocId && !includeEnd))
            {
                exceededEnd = true;
            }
            else
            {
                bool afterStart = key > startDocId || (key == startDocId && includeStart);
                bool beforeEnd = key < endDocId || (key == endDocId && includeEnd);

                if (afterStart && beforeEnd)
                {
                    results.Add(new BTreeEntry(key, leaf.LeafValues[i]));
                }
            }
        }

        return exceededEnd;
    }

    public bool Delete(int docId)
    {
        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            return DeleteFromNode(rootPageId, docId, rootAlreadyLocked: true);
        }
        finally
        {
            _rootLock.ExitWriteLock();
        }
    }

    public List<BTreeEntry> GetAllEntries()
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireReadLock(rootPageId);

            List<BTreeEntry> entries = new List<BTreeEntry>();
            CollectAllEntries(rootPageId, entries, rootAlreadyLocked: true);
            return entries;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<int> CollectAllPageIds()
    {
        _rootLock.EnterReadLock();
        List<int> pageIds = new List<int>();
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        Stack<int> pageStack = new Stack<int>();

        try
        {
            int rootPageId = _rootPageId;
            pageStack.Push(rootPageId);

            while (pageStack.Count > 0)
            {
                int currentPageId = pageStack.Pop();
                pageIds.Add(currentPageId);

                _pageLockManager.AcquireReadLock(currentPageId);
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                if (node.NodeType == BTreeNodeType.Internal)
                {
                    for (int i = node.KeyCount; i >= 0; i--)
                    {
                        pageStack.Push(node.ChildPageIds[i]);
                    }
                }
                _pageLockManager.ReleaseReadLock(currentPageId);
            }
        }
        finally
        {
            _rootLock.ExitReadLock();
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return pageIds;
    }

    private void CollectAllEntries(int pageId, List<BTreeEntry> entries, bool rootAlreadyLocked = false)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        Stack<int> pageStack = new Stack<int>();
        pageStack.Push(pageId);
        bool firstPage = true;

        try
        {
            while (pageStack.Count > 0)
            {
                int currentPageId = pageStack.Pop();
                if (firstPage && rootAlreadyLocked)
                {
                    firstPage = false;
                }
                else
                {
                    _pageLockManager.AcquireReadLock(currentPageId);
                }
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                if (node.NodeType == BTreeNodeType.Leaf)
                {
                    for (int i = 0; i < node.KeyCount; i++)
                    {
                        entries.Add(new BTreeEntry(node.Keys[i], node.LeafValues[i]));
                    }
                }
                else
                {
                    // Push children in reverse order so they're processed left-to-right
                    for (int i = node.KeyCount; i >= 0; i--)
                    {
                        pageStack.Push(node.ChildPageIds[i]);
                    }
                }
                _pageLockManager.ReleaseReadLock(currentPageId);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }
    }

    private bool DeleteFromNode(int pageId, int docId, bool rootAlreadyLocked = false)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        List<DeletePathEntry> path = ListPool<DeletePathEntry>.Rent(MAX_TREE_DEPTH);
        LockStack heldLocks = new LockStack(_pageLockManager, true);
        try
        {
            int currentPageId = pageId;
            int leafPageId = 0;
            if (!rootAlreadyLocked)
            {
                _pageLockManager.AcquireWriteLock(currentPageId);
            }
            heldLocks.Push(currentPageId);

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                int i = 0;
                while (i < node.KeyCount && docId > node.Keys[i])
                {
                    i++;
                }

                if (node.NodeType == BTreeNodeType.Leaf)
                {
                    if (i < node.KeyCount && node.Keys[i] == docId)
                    {
                        node.Keys.RemoveAt(i);
                        node.LeafValues.RemoveAt(i);
                        node.KeyCount--;

                        node.SerializeTo(buffer);
                        _pageIO.WritePage(currentPageId, buffer);

                        leafPageId = currentPageId;
                        result = true;
                    }
                    break;
                }
                else
                {
                    int childPageId = node.ChildPageIds[i];
                    _pageLockManager.AcquireWriteLock(childPageId);

                    // Read child to check if it's safe
                    byte[] childBuffer = BufferPool.Rent(_pageSize);
                    _pageIO.ReadPage(childPageId, childBuffer);

                    // Safe node optimization: if child won't underflow, release ancestor locks
                    // but keep parent (currentPageId) for potential rebalancing
                    if (BTreeNode.IsSafeForDelete(childBuffer, _order))
                    {
                        heldLocks.ReleaseAllExcept(currentPageId);
                        path.Clear();
                    }

                    BufferPool.Return(childBuffer);

                    path.Add(new DeletePathEntry(currentPageId, i));
                    heldLocks.Push(childPageId);
                    currentPageId = childPageId;
                }
            }

            if (result && leafPageId != 0 && node.IsUnderflow() && leafPageId != pageId)
            {
                RebalanceAfterDelete(path, leafPageId, node, heldLocks);
            }
        }
        finally
        {
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
            ListPool<DeletePathEntry>.Return(path);
        }

        return result;
    }

    private void RebalanceAfterDelete(List<DeletePathEntry> path, int nodePageId, BTreeNode node, LockStack heldLocks)
    {
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
        BTreeNode sibling = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        int siblingLockHeld = 0;

        try
        {
            int currentPageId = nodePageId;
            int pathIndex = path.Count - 1;

            while (pathIndex >= 0 && node.IsUnderflow())
            {
                DeletePathEntry parentEntry = path[pathIndex];
                int parentPageId = parentEntry.PageId;
                int childIndex = parentEntry.ChildIndex;

                _pageIO.ReadPage(parentPageId, parentBuffer);
                BTreeNode.DeserializeTo(parentBuffer, parent, _order);

                bool rebalanced = false;

                if (childIndex > 0)
                {
                    int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                    AcquireSiblingLock(leftSiblingPageId, heldLocks, ref siblingLockHeld);
                    _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromLeftSibling(parentPageId, parent, parentBuffer,
                                              leftSiblingPageId, sibling, siblingBuffer,
                                              currentPageId, node, childIndex);
                        rebalanced = true;
                    }
                    ReleaseSiblingLock(ref siblingLockHeld);
                }

                if (!rebalanced && childIndex < parent.KeyCount)
                {
                    int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                    AcquireSiblingLock(rightSiblingPageId, heldLocks, ref siblingLockHeld);
                    _pageIO.ReadPage(rightSiblingPageId, siblingBuffer);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromRightSibling(parentPageId, parent, parentBuffer,
                                               currentPageId, node,
                                               rightSiblingPageId, sibling, siblingBuffer,
                                               childIndex);
                        rebalanced = true;
                    }
                    ReleaseSiblingLock(ref siblingLockHeld);
                }

                if (!rebalanced)
                {
                    if (childIndex > 0)
                    {
                        int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                        AcquireSiblingLock(leftSiblingPageId, heldLocks, ref siblingLockHeld);
                        _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithLeftSibling(parentPageId, parent, parentBuffer,
                                             leftSiblingPageId, sibling, siblingBuffer,
                                             currentPageId, node, childIndex);

                        ReleaseSiblingLock(ref siblingLockHeld);
                        currentPageId = parentPageId;
                        _pageIO.ReadPage(parentPageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                    else
                    {
                        int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                        AcquireSiblingLock(rightSiblingPageId, heldLocks, ref siblingLockHeld);
                        _pageIO.ReadPage(rightSiblingPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithRightSibling(parentPageId, parent, parentBuffer,
                                              currentPageId, node,
                                              rightSiblingPageId, sibling, siblingBuffer,
                                              childIndex);

                        ReleaseSiblingLock(ref siblingLockHeld);
                        currentPageId = parentPageId;
                        _pageIO.ReadPage(parentPageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                }
                else
                {
                    break;
                }

                pathIndex--;
            }

            if (pathIndex < 0 && node.NodeType == BTreeNodeType.Internal && node.KeyCount == 0)
            {
                // Already holding _rootLock from Delete()
                int newRootPageId = node.ChildPageIds[0];
                _pageManager.DeallocatePage(_rootPageId);
                _rootPageId = newRootPageId;
            }
        }
        finally
        {
            ReleaseSiblingLock(ref siblingLockHeld);
            BufferPool.Return(parentBuffer);
            BufferPool.Return(siblingBuffer);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(sibling);
        }
    }

    private void AcquireSiblingLock(int siblingPageId, LockStack heldLocks, ref int siblingLockHeld)
    {
        // Throw if sibling is already held (would cause deadlock)
        if (heldLocks.Contains(siblingPageId))
        {
            throw new InvalidOperationException($"Sibling page {siblingPageId} is already in heldLocks - this indicates tree corruption (duplicate child pointers)");
        }

        int minHeld = heldLocks.GetMinPageId();
        if (siblingPageId < minHeld)
        {
            // Need to release all and re-acquire in sorted order
            int[] pageIds = new int[heldLocks.Count + 1];
            int count = heldLocks.CopyTo(pageIds);

            // Save original page IDs before AcquireWriteLocks sorts the array
            int[] originalPageIds = new int[count];
            Array.Copy(pageIds, originalPageIds, count);

            pageIds[count] = siblingPageId;
            heldLocks.ReleaseAll();

            _pageLockManager.AcquireWriteLocks(pageIds, count + 1);

            // Rebuild heldLocks from original (without sibling - it's tracked separately)
            for (int i = 0; i < count; i++)
            {
                heldLocks.Push(originalPageIds[i]);
            }
        }
        else
        {
            _pageLockManager.AcquireWriteLock(siblingPageId);
        }
        siblingLockHeld = siblingPageId;
    }

    private void ReleaseSiblingLock(ref int siblingLockHeld)
    {
        if (siblingLockHeld != 0)
        {
            _pageLockManager.ReleaseWriteLock(siblingLockHeld);
            siblingLockHeld = 0;
        }
    }

    private void BorrowFromLeftSibling(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                       int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                       int currentPageId, BTreeNode current, int childIndex)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                int borrowedKey = leftSibling.Keys[leftSibling.KeyCount - 1];
                DocumentLocation borrowedValue = leftSibling.LeafValues[leftSibling.KeyCount - 1];

                current.Keys.Insert(0, borrowedKey);
                current.LeafValues.Insert(0, borrowedValue);
                current.KeyCount++;

                leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.LeafValues.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.KeyCount--;

                parent.Keys[childIndex - 1] = leftSibling.Keys[leftSibling.KeyCount - 1];
            }
            else
            {
                int separatorKey = parent.Keys[childIndex - 1];

                current.Keys.Insert(0, separatorKey);
                current.ChildPageIds.Insert(0, leftSibling.ChildPageIds[leftSibling.KeyCount]);
                current.KeyCount++;

                parent.Keys[childIndex - 1] = leftSibling.Keys[leftSibling.KeyCount - 1];

                leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.ChildPageIds.RemoveAt(leftSibling.KeyCount);
                leftSibling.KeyCount--;
            }

            leftSibling.SerializeTo(leftBuffer);
            _pageIO.WritePage(leftSiblingPageId, leftBuffer);

            current.SerializeTo(currentBuffer);
            _pageIO.WritePage(currentPageId, currentBuffer);

            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }
    }

    private void BorrowFromRightSibling(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                        int currentPageId, BTreeNode current,
                                        int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                        int childIndex)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                int borrowedKey = rightSibling.Keys[0];
                DocumentLocation borrowedValue = rightSibling.LeafValues[0];

                current.Keys.Add(borrowedKey);
                current.LeafValues.Add(borrowedValue);
                current.KeyCount++;

                rightSibling.Keys.RemoveAt(0);
                rightSibling.LeafValues.RemoveAt(0);
                rightSibling.KeyCount--;

                parent.Keys[childIndex] = borrowedKey;
            }
            else
            {
                int separatorKey = parent.Keys[childIndex];

                current.Keys.Add(separatorKey);
                current.ChildPageIds.Add(rightSibling.ChildPageIds[0]);
                current.KeyCount++;

                parent.Keys[childIndex] = rightSibling.Keys[0];

                rightSibling.Keys.RemoveAt(0);
                rightSibling.ChildPageIds.RemoveAt(0);
                rightSibling.KeyCount--;
            }

            current.SerializeTo(currentBuffer);
            _pageIO.WritePage(currentPageId, currentBuffer);

            rightSibling.SerializeTo(rightBuffer);
            _pageIO.WritePage(rightSiblingPageId, rightBuffer);

            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }
    }

    private void MergeWithLeftSibling(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                      int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                      int currentPageId, BTreeNode current, int childIndex)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int i = 0; i < current.KeyCount; i++)
            {
                leftSibling.Keys.Add(current.Keys[i]);
                leftSibling.LeafValues.Add(current.LeafValues[i]);
            }
            leftSibling.KeyCount += current.KeyCount;

            leftSibling.NextLeaf = current.NextLeaf;
        }
        else
        {
            leftSibling.Keys.Add(parent.Keys[childIndex - 1]);
            leftSibling.KeyCount++;

            for (int i = 0; i < current.KeyCount; i++)
            {
                leftSibling.Keys.Add(current.Keys[i]);
            }
            for (int i = 0; i <= current.KeyCount; i++)
            {
                leftSibling.ChildPageIds.Add(current.ChildPageIds[i]);
            }
            leftSibling.KeyCount += current.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex - 1);
        parent.ChildPageIds.RemoveAt(childIndex);
        parent.KeyCount--;

        leftSibling.SerializeTo(leftBuffer);
        _pageIO.WritePage(leftSiblingPageId, leftBuffer);

        parent.SerializeTo(parentBuffer);
        _pageIO.WritePage(parentPageId, parentBuffer);

        _pageManager.DeallocatePage(currentPageId);
    }

    private void MergeWithRightSibling(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                       int currentPageId, BTreeNode current,
                                       int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                       int childIndex)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int i = 0; i < rightSibling.KeyCount; i++)
            {
                current.Keys.Add(rightSibling.Keys[i]);
                current.LeafValues.Add(rightSibling.LeafValues[i]);
            }
            current.KeyCount += rightSibling.KeyCount;

            current.NextLeaf = rightSibling.NextLeaf;
        }
        else
        {
            current.Keys.Add(parent.Keys[childIndex]);
            current.KeyCount++;

            for (int i = 0; i < rightSibling.KeyCount; i++)
            {
                current.Keys.Add(rightSibling.Keys[i]);
            }
            for (int i = 0; i <= rightSibling.KeyCount; i++)
            {
                current.ChildPageIds.Add(rightSibling.ChildPageIds[i]);
            }
            current.KeyCount += rightSibling.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex);
        parent.ChildPageIds.RemoveAt(childIndex + 1);
        parent.KeyCount--;

        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            current.SerializeTo(currentBuffer);
            _pageIO.WritePage(currentPageId, currentBuffer);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }

        parent.SerializeTo(parentBuffer);
        _pageIO.WritePage(parentPageId, parentBuffer);

        _pageManager.DeallocatePage(rightSiblingPageId);
    }

    private DocumentLocation? SearchNode(int pageId, int docId, bool rootAlreadyLocked = false)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;
            if (!rootAlreadyLocked)
            {
                _pageLockManager.AcquireReadLock(currentPageId);
            }

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);
                int pos = BTreeNode.FindKeyPosition(buffer, keyCount, docId);

                if (BTreeNode.IsLeafNode(buffer))
                {
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        result = BTreeNode.GetLeafValue(buffer, keyCount, pos);
                    }
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    break;
                }
                else
                {
                    int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, pos);
                    _pageLockManager.AcquireReadLock(childPageId);
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = childPageId;
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private void InsertNonFull(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, LockStack heldLocks)
    {
        int currentPageId = pageId;

        while (true)
        {
            ushort keyCount = BTreeNode.GetKeyCount(buffer);

            if (BTreeNode.IsLeafNode(buffer))
            {
                // Insert directly into the buffer
                BTreeNode.InsertIntoLeaf(buffer, keyCount, docId, location, _order);
                _pageIO.WritePage(currentPageId, buffer);
                break;
            }
            else
            {
                // Find child to descend into
                int childIndex = BTreeNode.FindChildIndex(buffer, keyCount, docId);
                int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                // Acquire lock on child before reading
                _pageLockManager.AcquireWriteLock(childPageId);
                _pageIO.ReadPage(childPageId, childBuffer);

                // Safe node optimization: if child won't split, release all ancestor locks
                if (BTreeNode.IsSafeForInsert(childBuffer, _order))
                {
                    heldLocks.ReleaseAll();
                }

                if (BTreeNode.IsNodeFull(childBuffer, _order))
                {
                    // Need to split child - we hold locks on parent and child
                    SplitChild(currentPageId, childIndex, childPageId);

                    // Re-read current node after split to get updated key
                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = BTreeNode.GetKeyCount(buffer);

                    // Check which child to descend into after split
                    int newChildPageId;
                    if (docId > BTreeNode.GetKey(buffer, childIndex))
                    {
                        childIndex++;
                        newChildPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);
                        // The new child was created by split, need to acquire its lock
                        _pageLockManager.AcquireWriteLock(newChildPageId);
                        _pageLockManager.ReleaseWriteLock(childPageId);
                        childPageId = newChildPageId;
                    }

                    // Read the correct child after split
                    _pageIO.ReadPage(childPageId, childBuffer);
                }

                heldLocks.Push(childPageId);

                // Swap buffers: childBuffer becomes buffer for next iteration
                byte[] temp = buffer;
                buffer = childBuffer;
                childBuffer = temp;
                currentPageId = childPageId;
            }
        }
    }

    private void SplitChild(int parentPageId, int index, int childPageId)
    {
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        BTreeNode fullChild = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        BTreeNode newChild = null;
        int newChildPageId = 0;
        try
        {
            _pageIO.ReadPage(childPageId, childBuffer);
            BTreeNode.DeserializeTo(childBuffer, fullChild, _order);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            BTreeNode.DeserializeTo(parentBuffer, parent, _order);

            newChildPageId = _pageManager.AllocatePage();
            _pageLockManager.AcquireWriteLock(newChildPageId);
            newChild = BTreeNodePool.Rent(_pageSize, _order, fullChild.NodeType);

            int mid = (_order - 1) / 2;
            int keyToPromote = fullChild.Keys[mid];

            for (int j = mid + 1; j < fullChild.KeyCount; j++)
            {
                newChild.Keys.Add(fullChild.Keys[j]);

                if (fullChild.NodeType == BTreeNodeType.Leaf)
                {
                    newChild.LeafValues.Add(fullChild.LeafValues[j]);
                }
                else
                {
                    newChild.ChildPageIds.Add(fullChild.ChildPageIds[j]);
                }

                newChild.KeyCount++;
            }

            if (fullChild.NodeType == BTreeNodeType.Internal)
            {
                newChild.ChildPageIds.Add(fullChild.ChildPageIds[fullChild.KeyCount]);
            }

            int originalKeyCount = fullChild.KeyCount;

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                // For leaf nodes, keep the middle key (B+ tree: leaves hold all data)
                fullChild.KeyCount = (ushort)(mid + 1);
                int keysToRemove = originalKeyCount - (mid + 1);

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid + 1, keysToRemove);
                    fullChild.LeafValues.RemoveRange(mid + 1, keysToRemove);
                }
                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                // For internal nodes, the middle key is promoted (removed from child)
                fullChild.KeyCount = (ushort)mid;
                int keysToRemove = originalKeyCount - mid;

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid, keysToRemove);
                }

                // Keep mid+1 children for mid keys
                int childPointersToRemove = fullChild.ChildPageIds.Count - (mid + 1);
                if (childPointersToRemove > 0)
                {
                    fullChild.ChildPageIds.RemoveRange(mid + 1, childPointersToRemove);
                }
            }

            parent.ChildPageIds.Insert(index + 1, newChildPageId);
            parent.Keys.Insert(index, keyToPromote);
            parent.KeyCount++;

            fullChild.SerializeTo(childBuffer);
            _pageIO.WritePage(childPageId, childBuffer);

            newChild.SerializeTo(childBuffer);
            _pageIO.WritePage(newChildPageId, childBuffer);

            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            if (newChildPageId != 0)
            {
                _pageLockManager.ReleaseWriteLock(newChildPageId);
            }
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
            BTreeNodePool.Return(fullChild);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(newChild);
        }
    }

    public async Task<DocumentLocation?> SearchAsync(int docId, CancellationToken cancellationToken = default)
    {
        await _rootLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int rootPageId = _rootPageId;
            await _pageLockManager.AcquireReadLockAsync(rootPageId, cancellationToken).ConfigureAwait(false);
            return await SearchNodeAsync(rootPageId, docId, cancellationToken, rootAlreadyLocked: true).ConfigureAwait(false);
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public async Task<List<BTreeEntry>> SearchRangeAsync(int startDocId, int endDocId, bool includeStart, bool includeEnd, CancellationToken cancellationToken = default)
    {
        List<BTreeEntry> results = new List<BTreeEntry>();

        await _rootLock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int rootPageId = _rootPageId;
            await _pageLockManager.AcquireReadLockAsync(rootPageId, cancellationToken).ConfigureAwait(false);

            // FindLeafForKeyAsync returns with read lock held on the leaf
            int leafPageId = await FindLeafForKeyAsync(rootPageId, startDocId, buffer, node, cancellationToken, rootAlreadyLocked: true).ConfigureAwait(false);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                // Read lock already held on currentPageId
                await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(buffer, node, _order);

                bool exceededEnd = ScanLeafForRange(node, startDocId, endDocId, includeStart, includeEnd, results);

                if (exceededEnd || node.NextLeaf == 0)
                {
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    continueScanning = false;
                }
                else
                {
                    int nextPageId = node.NextLeaf;
                    await _pageLockManager.AcquireReadLockAsync(nextPageId, cancellationToken).ConfigureAwait(false);
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = nextPageId;
                }
            }
        }
        finally
        {
            _rootLock.ExitReadLock();
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return results;
    }

    /// <summary>
    /// Finds the leaf containing the target key. Returns with read lock held on the leaf.
    /// </summary>
    private async Task<int> FindLeafForKeyAsync(int pageId, int targetKey, byte[] buffer, BTreeNode node, CancellationToken cancellationToken, bool rootAlreadyLocked = false)
    {
        int currentPageId = pageId;
        if (!rootAlreadyLocked)
        {
            await _pageLockManager.AcquireReadLockAsync(currentPageId, cancellationToken).ConfigureAwait(false);
        }

        while (currentPageId != 0)
        {
            await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                // Return with read lock still held
                break;
            }

            int i = 0;
            while (i < node.KeyCount && targetKey > node.Keys[i])
            {
                i++;
            }

            int childPageId = node.ChildPageIds[i];
            await _pageLockManager.AcquireReadLockAsync(childPageId, cancellationToken).ConfigureAwait(false);
            _pageLockManager.ReleaseReadLock(currentPageId);
            currentPageId = childPageId;
        }

        return currentPageId;
    }

    public async Task InsertAsync(int docId, DocumentLocation location, CancellationToken cancellationToken = default)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        BTreeNode newRoot = null;
        LockStack heldLocks = new LockStack(_pageLockManager, true);

        await _rootLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int rootPageId = _rootPageId;
            await _pageLockManager.AcquireWriteLockAsync(rootPageId, cancellationToken).ConfigureAwait(false);
            await _pageIO.ReadPageAsync(rootPageId, buffer, cancellationToken).ConfigureAwait(false);

            if (BTreeNode.IsNodeFull(buffer, _order))
            {
                // Root is full - need to split it first
                BTreeNode root = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
                try
                {
                    BTreeNode.DeserializeTo(buffer, root, _order);

                    int newRootPageId = _pageManager.AllocatePage();
                    await _pageLockManager.AcquireWriteLockAsync(newRootPageId, cancellationToken).ConfigureAwait(false);
                    newRoot = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
                    newRoot.ChildPageIds.Add(rootPageId);

                    newRoot.SerializeTo(buffer);
                    await _pageIO.WritePageAsync(newRootPageId, buffer, cancellationToken).ConfigureAwait(false);

                    await SplitChildAsync(newRootPageId, 0, rootPageId, cancellationToken).ConfigureAwait(false);
                    _rootPageId = newRootPageId;

                    // Release rootPageId lock - InsertNonFullAsync will acquire child locks as needed
                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    // Re-read the new root for InsertNonFullAsync
                    await _pageIO.ReadPageAsync(newRootPageId, buffer, cancellationToken).ConfigureAwait(false);
                    await InsertNonFullAsync(newRootPageId, buffer, childBuffer, docId, location, heldLocks, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    BTreeNodePool.Return(root);
                }
            }
            else
            {
                // Root won't change, keep page lock
                heldLocks.Push(rootPageId);
                await InsertNonFullAsync(rootPageId, buffer, childBuffer, docId, location, heldLocks, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _rootLock.ExitWriteLock();
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
            BTreeNodePool.Return(newRoot);
        }
    }

    public async Task<bool> DeleteAsync(int docId, CancellationToken cancellationToken = default)
    {
        await _rootLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int rootPageId = _rootPageId;
            await _pageLockManager.AcquireWriteLockAsync(rootPageId, cancellationToken).ConfigureAwait(false);
            return await DeleteFromNodeAsync(rootPageId, docId, cancellationToken, rootAlreadyLocked: true).ConfigureAwait(false);
        }
        finally
        {
            _rootLock.ExitWriteLock();
        }
    }

    private async Task<DocumentLocation?> SearchNodeAsync(int pageId, int docId, CancellationToken cancellationToken, bool rootAlreadyLocked = false)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;
            if (!rootAlreadyLocked)
            {
                await _pageLockManager.AcquireReadLockAsync(currentPageId, cancellationToken).ConfigureAwait(false);
            }

            while (currentPageId != 0)
            {
                await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);
                int pos = BTreeNode.FindKeyPosition(buffer, keyCount, docId);

                if (BTreeNode.IsLeafNode(buffer))
                {
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        result = BTreeNode.GetLeafValue(buffer, keyCount, pos);
                    }
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, pos);
                    await _pageLockManager.AcquireReadLockAsync(childPageId, cancellationToken).ConfigureAwait(false);
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = childPageId;
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private async Task InsertNonFullAsync(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, LockStack heldLocks, CancellationToken cancellationToken)
    {
        int currentPageId = pageId;

        while (true)
        {
            ushort keyCount = BTreeNode.GetKeyCount(buffer);

            if (BTreeNode.IsLeafNode(buffer))
            {
                // Insert directly into the buffer
                BTreeNode.InsertIntoLeaf(buffer, keyCount, docId, location, _order);
                await _pageIO.WritePageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                break;
            }
            else
            {
                // Find child to descend into
                int childIndex = BTreeNode.FindChildIndex(buffer, keyCount, docId);
                int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                // Acquire lock on child before reading
                await _pageLockManager.AcquireWriteLockAsync(childPageId, cancellationToken).ConfigureAwait(false);
                await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);

                // Safe node optimization: if child won't split, release all ancestor locks
                if (BTreeNode.IsSafeForInsert(childBuffer, _order))
                {
                    heldLocks.ReleaseAll();
                }

                if (BTreeNode.IsNodeFull(childBuffer, _order))
                {
                    // Need to split child - we hold locks on parent and child
                    await SplitChildAsync(currentPageId, childIndex, childPageId, cancellationToken).ConfigureAwait(false);

                    // Re-read current node after split to get updated key
                    await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                    keyCount = BTreeNode.GetKeyCount(buffer);

                    // Check which child to descend into after split
                    int newChildPageId;
                    if (docId > BTreeNode.GetKey(buffer, childIndex))
                    {
                        childIndex++;
                        newChildPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);
                        // The new child was created by split, need to acquire its lock
                        await _pageLockManager.AcquireWriteLockAsync(newChildPageId, cancellationToken).ConfigureAwait(false);
                        _pageLockManager.ReleaseWriteLock(childPageId);
                        childPageId = newChildPageId;
                    }

                    // Read the correct child after split
                    await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
                }

                heldLocks.Push(childPageId);

                // Swap buffers: childBuffer becomes buffer for next iteration
                byte[] temp = buffer;
                buffer = childBuffer;
                childBuffer = temp;
                currentPageId = childPageId;
            }
        }
    }

    private async Task SplitChildAsync(int parentPageId, int index, int childPageId, CancellationToken cancellationToken)
    {
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        BTreeNode fullChild = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        BTreeNode newChild = null;
        int newChildPageId = 0;
        try
        {
            await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(childBuffer, fullChild, _order);

            await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(parentBuffer, parent, _order);

            newChildPageId = _pageManager.AllocatePage();
            await _pageLockManager.AcquireWriteLockAsync(newChildPageId, cancellationToken).ConfigureAwait(false);
            newChild = BTreeNodePool.Rent(_pageSize, _order, fullChild.NodeType);

            int mid = (_order - 1) / 2;
            int keyToPromote = fullChild.Keys[mid];

            for (int j = mid + 1; j < fullChild.KeyCount; j++)
            {
                newChild.Keys.Add(fullChild.Keys[j]);

                if (fullChild.NodeType == BTreeNodeType.Leaf)
                {
                    newChild.LeafValues.Add(fullChild.LeafValues[j]);
                }
                else
                {
                    newChild.ChildPageIds.Add(fullChild.ChildPageIds[j]);
                }

                newChild.KeyCount++;
            }

            if (fullChild.NodeType == BTreeNodeType.Internal)
            {
                newChild.ChildPageIds.Add(fullChild.ChildPageIds[fullChild.KeyCount]);
            }

            int originalKeyCount = fullChild.KeyCount;

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                // For leaf nodes, keep the middle key (B+ tree: leaves hold all data)
                fullChild.KeyCount = (ushort)(mid + 1);
                int keysToRemove = originalKeyCount - (mid + 1);

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid + 1, keysToRemove);
                    fullChild.LeafValues.RemoveRange(mid + 1, keysToRemove);
                }
                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                // For internal nodes, the middle key is promoted (removed from child)
                fullChild.KeyCount = (ushort)mid;
                int keysToRemove = originalKeyCount - mid;

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid, keysToRemove);
                }

                // Keep mid+1 children for mid keys
                int childPointersToRemove = fullChild.ChildPageIds.Count - (mid + 1);
                if (childPointersToRemove > 0)
                {
                    fullChild.ChildPageIds.RemoveRange(mid + 1, childPointersToRemove);
                }
            }

            parent.ChildPageIds.Insert(index + 1, newChildPageId);
            parent.Keys.Insert(index, keyToPromote);
            parent.KeyCount++;

            fullChild.SerializeTo(childBuffer);
            await _pageIO.WritePageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);

            newChild.SerializeTo(childBuffer);
            await _pageIO.WritePageAsync(newChildPageId, childBuffer, cancellationToken).ConfigureAwait(false);

            parent.SerializeTo(parentBuffer);
            await _pageIO.WritePageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (newChildPageId != 0)
            {
                _pageLockManager.ReleaseWriteLock(newChildPageId);
            }
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
            BTreeNodePool.Return(fullChild);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(newChild);
        }
    }

    private async Task<bool> DeleteFromNodeAsync(int pageId, int docId, CancellationToken cancellationToken, bool rootAlreadyLocked = false)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        List<DeletePathEntry> path = ListPool<DeletePathEntry>.Rent(MAX_TREE_DEPTH);
        LockStack heldLocks = new LockStack(_pageLockManager, true);
        try
        {
            int currentPageId = pageId;
            int leafPageId = 0;
            if (!rootAlreadyLocked)
            {
                await _pageLockManager.AcquireWriteLockAsync(currentPageId, cancellationToken).ConfigureAwait(false);
            }
            heldLocks.Push(currentPageId);

            while (currentPageId != 0)
            {
                await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(buffer, node, _order);

                int i = 0;
                while (i < node.KeyCount && docId > node.Keys[i])
                {
                    i++;
                }

                if (node.NodeType == BTreeNodeType.Leaf)
                {
                    if (i < node.KeyCount && node.Keys[i] == docId)
                    {
                        node.Keys.RemoveAt(i);
                        node.LeafValues.RemoveAt(i);
                        node.KeyCount--;

                        node.SerializeTo(buffer);
                        await _pageIO.WritePageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);

                        leafPageId = currentPageId;
                        result = true;
                    }
                    break;
                }
                else
                {
                    int childPageId = node.ChildPageIds[i];
                    await _pageLockManager.AcquireWriteLockAsync(childPageId, cancellationToken).ConfigureAwait(false);

                    // Read child to check if it's safe
                    byte[] childBuffer = BufferPool.Rent(_pageSize);
                    await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);

                    // Safe node optimization: if child won't underflow, release ancestor locks
                    // but keep parent (currentPageId) for potential rebalancing
                    if (BTreeNode.IsSafeForDelete(childBuffer, _order))
                    {
                        heldLocks.ReleaseAllExcept(currentPageId);
                        path.Clear();
                    }

                    BufferPool.Return(childBuffer);

                    path.Add(new DeletePathEntry(currentPageId, i));
                    heldLocks.Push(childPageId);
                    currentPageId = childPageId;
                }
            }

            if (result && leafPageId != 0 && node.IsUnderflow() && leafPageId != pageId)
            {
                await RebalanceAfterDeleteAsync(path, leafPageId, node, heldLocks, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
            ListPool<DeletePathEntry>.Return(path);
        }

        return result;
    }

    private async Task RebalanceAfterDeleteAsync(List<DeletePathEntry> path, int nodePageId, BTreeNode node, LockStack heldLocks, CancellationToken cancellationToken)
    {
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
        BTreeNode sibling = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        int siblingLockHeld = 0;

        try
        {
            int currentPageId = nodePageId;
            int pathIndex = path.Count - 1;

            while (pathIndex >= 0 && node.IsUnderflow())
            {
                DeletePathEntry parentEntry = path[pathIndex];
                int parentPageId = parentEntry.PageId;
                int childIndex = parentEntry.ChildIndex;

                await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(parentBuffer, parent, _order);

                bool rebalanced = false;

                if (childIndex > 0)
                {
                    int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                    await AcquireSiblingLockAsync(leftSiblingPageId, heldLocks, cancellationToken).ConfigureAwait(false);
                    siblingLockHeld = leftSiblingPageId;
                    await _pageIO.ReadPageAsync(leftSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        await BorrowFromLeftSiblingAsync(parentPageId, parent, parentBuffer,
                                              leftSiblingPageId, sibling, siblingBuffer,
                                              currentPageId, node, childIndex, cancellationToken).ConfigureAwait(false);
                        rebalanced = true;
                    }
                    ReleaseSiblingLock(ref siblingLockHeld);
                }

                if (!rebalanced && childIndex < parent.KeyCount)
                {
                    int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                    await AcquireSiblingLockAsync(rightSiblingPageId, heldLocks, cancellationToken).ConfigureAwait(false);
                    siblingLockHeld = rightSiblingPageId;
                    await _pageIO.ReadPageAsync(rightSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        await BorrowFromRightSiblingAsync(parentPageId, parent, parentBuffer,
                                               currentPageId, node,
                                               rightSiblingPageId, sibling, siblingBuffer,
                                               childIndex, cancellationToken).ConfigureAwait(false);
                        rebalanced = true;
                    }
                    ReleaseSiblingLock(ref siblingLockHeld);
                }

                if (!rebalanced)
                {
                    if (childIndex > 0)
                    {
                        int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                        await AcquireSiblingLockAsync(leftSiblingPageId, heldLocks, cancellationToken).ConfigureAwait(false);
                        siblingLockHeld = leftSiblingPageId;
                        await _pageIO.ReadPageAsync(leftSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        await MergeWithLeftSiblingAsync(parentPageId, parent, parentBuffer,
                                             leftSiblingPageId, sibling, siblingBuffer,
                                             currentPageId, node, childIndex, cancellationToken).ConfigureAwait(false);

                        ReleaseSiblingLock(ref siblingLockHeld);
                        currentPageId = parentPageId;
                        await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                    else
                    {
                        int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                        await AcquireSiblingLockAsync(rightSiblingPageId, heldLocks, cancellationToken).ConfigureAwait(false);
                        siblingLockHeld = rightSiblingPageId;
                        await _pageIO.ReadPageAsync(rightSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        await MergeWithRightSiblingAsync(parentPageId, parent, parentBuffer,
                                              currentPageId, node,
                                              rightSiblingPageId, sibling, siblingBuffer,
                                              childIndex, cancellationToken).ConfigureAwait(false);

                        ReleaseSiblingLock(ref siblingLockHeld);
                        currentPageId = parentPageId;
                        await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                }
                else
                {
                    break;
                }

                pathIndex--;
            }

            if (pathIndex < 0 && node.NodeType == BTreeNodeType.Internal && node.KeyCount == 0)
            {
                // Already holding _rootLock from DeleteAsync()
                int newRootPageId = node.ChildPageIds[0];
                _pageManager.DeallocatePage(_rootPageId);
                _rootPageId = newRootPageId;
            }
        }
        finally
        {
            ReleaseSiblingLock(ref siblingLockHeld);
            BufferPool.Return(parentBuffer);
            BufferPool.Return(siblingBuffer);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(sibling);
        }
    }

    private async Task AcquireSiblingLockAsync(int siblingPageId, LockStack heldLocks, CancellationToken cancellationToken)
    {
        // Throw if sibling is already held (would cause deadlock)
        if (heldLocks.Contains(siblingPageId))
        {
            throw new InvalidOperationException($"Sibling page {siblingPageId} is already in heldLocks - this indicates tree corruption (duplicate child pointers)");
        }

        int minHeld = heldLocks.GetMinPageId();
        if (siblingPageId < minHeld)
        {
            // Need to release all and re-acquire in sorted order
            int[] pageIds = new int[heldLocks.Count + 1];
            int count = heldLocks.CopyTo(pageIds);

            // Save original page IDs before AcquireWriteLocksAsync sorts the array
            int[] originalPageIds = new int[count];
            Array.Copy(pageIds, originalPageIds, count);

            pageIds[count] = siblingPageId;
            heldLocks.ReleaseAll();

            await _pageLockManager.AcquireWriteLocksAsync(pageIds, count + 1, cancellationToken).ConfigureAwait(false);

            // Rebuild heldLocks from original (without sibling - it's tracked separately)
            for (int i = 0; i < count; i++)
            {
                heldLocks.Push(originalPageIds[i]);
            }
        }
        else
        {
            await _pageLockManager.AcquireWriteLockAsync(siblingPageId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task BorrowFromLeftSiblingAsync(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                       int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                       int currentPageId, BTreeNode current, int childIndex,
                                       CancellationToken cancellationToken)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                int borrowedKey = leftSibling.Keys[leftSibling.KeyCount - 1];
                DocumentLocation borrowedValue = leftSibling.LeafValues[leftSibling.KeyCount - 1];

                current.Keys.Insert(0, borrowedKey);
                current.LeafValues.Insert(0, borrowedValue);
                current.KeyCount++;

                leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.LeafValues.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.KeyCount--;

                parent.Keys[childIndex - 1] = leftSibling.Keys[leftSibling.KeyCount - 1];
            }
            else
            {
                int separatorKey = parent.Keys[childIndex - 1];

                current.Keys.Insert(0, separatorKey);
                current.ChildPageIds.Insert(0, leftSibling.ChildPageIds[leftSibling.KeyCount]);
                current.KeyCount++;

                parent.Keys[childIndex - 1] = leftSibling.Keys[leftSibling.KeyCount - 1];

                leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
                leftSibling.ChildPageIds.RemoveAt(leftSibling.KeyCount);
                leftSibling.KeyCount--;
            }

            leftSibling.SerializeTo(leftBuffer);
            await _pageIO.WritePageAsync(leftSiblingPageId, leftBuffer, cancellationToken).ConfigureAwait(false);

            current.SerializeTo(currentBuffer);
            await _pageIO.WritePageAsync(currentPageId, currentBuffer, cancellationToken).ConfigureAwait(false);

            parent.SerializeTo(parentBuffer);
            await _pageIO.WritePageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }
    }

    private async Task BorrowFromRightSiblingAsync(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                        int currentPageId, BTreeNode current,
                                        int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                        int childIndex, CancellationToken cancellationToken)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                int borrowedKey = rightSibling.Keys[0];
                DocumentLocation borrowedValue = rightSibling.LeafValues[0];

                current.Keys.Add(borrowedKey);
                current.LeafValues.Add(borrowedValue);
                current.KeyCount++;

                rightSibling.Keys.RemoveAt(0);
                rightSibling.LeafValues.RemoveAt(0);
                rightSibling.KeyCount--;

                parent.Keys[childIndex] = borrowedKey;
            }
            else
            {
                int separatorKey = parent.Keys[childIndex];

                current.Keys.Add(separatorKey);
                current.ChildPageIds.Add(rightSibling.ChildPageIds[0]);
                current.KeyCount++;

                parent.Keys[childIndex] = rightSibling.Keys[0];

                rightSibling.Keys.RemoveAt(0);
                rightSibling.ChildPageIds.RemoveAt(0);
                rightSibling.KeyCount--;
            }

            current.SerializeTo(currentBuffer);
            await _pageIO.WritePageAsync(currentPageId, currentBuffer, cancellationToken).ConfigureAwait(false);

            rightSibling.SerializeTo(rightBuffer);
            await _pageIO.WritePageAsync(rightSiblingPageId, rightBuffer, cancellationToken).ConfigureAwait(false);

            parent.SerializeTo(parentBuffer);
            await _pageIO.WritePageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }
    }

    private async Task MergeWithLeftSiblingAsync(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                      int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                      int currentPageId, BTreeNode current, int childIndex,
                                      CancellationToken cancellationToken)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int i = 0; i < current.KeyCount; i++)
            {
                leftSibling.Keys.Add(current.Keys[i]);
                leftSibling.LeafValues.Add(current.LeafValues[i]);
            }
            leftSibling.KeyCount += current.KeyCount;

            leftSibling.NextLeaf = current.NextLeaf;
        }
        else
        {
            leftSibling.Keys.Add(parent.Keys[childIndex - 1]);
            leftSibling.KeyCount++;

            for (int i = 0; i < current.KeyCount; i++)
            {
                leftSibling.Keys.Add(current.Keys[i]);
            }
            for (int i = 0; i <= current.KeyCount; i++)
            {
                leftSibling.ChildPageIds.Add(current.ChildPageIds[i]);
            }
            leftSibling.KeyCount += current.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex - 1);
        parent.ChildPageIds.RemoveAt(childIndex);
        parent.KeyCount--;

        leftSibling.SerializeTo(leftBuffer);
        await _pageIO.WritePageAsync(leftSiblingPageId, leftBuffer, cancellationToken).ConfigureAwait(false);

        parent.SerializeTo(parentBuffer);
        await _pageIO.WritePageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);

        _pageManager.DeallocatePage(currentPageId);
    }

    private async Task MergeWithRightSiblingAsync(int parentPageId, BTreeNode parent, byte[] parentBuffer,
                                       int currentPageId, BTreeNode current,
                                       int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                       int childIndex, CancellationToken cancellationToken)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int i = 0; i < rightSibling.KeyCount; i++)
            {
                current.Keys.Add(rightSibling.Keys[i]);
                current.LeafValues.Add(rightSibling.LeafValues[i]);
            }
            current.KeyCount += rightSibling.KeyCount;

            current.NextLeaf = rightSibling.NextLeaf;
        }
        else
        {
            current.Keys.Add(parent.Keys[childIndex]);
            current.KeyCount++;

            for (int i = 0; i < rightSibling.KeyCount; i++)
            {
                current.Keys.Add(rightSibling.Keys[i]);
            }
            for (int i = 0; i <= rightSibling.KeyCount; i++)
            {
                current.ChildPageIds.Add(rightSibling.ChildPageIds[i]);
            }
            current.KeyCount += rightSibling.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex);
        parent.ChildPageIds.RemoveAt(childIndex + 1);
        parent.KeyCount--;

        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            current.SerializeTo(currentBuffer);
            await _pageIO.WritePageAsync(currentPageId, currentBuffer, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            BufferPool.Return(currentBuffer);
        }

        parent.SerializeTo(parentBuffer);
        await _pageIO.WritePageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);

        _pageManager.DeallocatePage(rightSiblingPageId);
    }
}
