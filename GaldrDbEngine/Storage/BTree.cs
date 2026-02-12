using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Transactions;
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
    private readonly Dictionary<int, List<PendingLeafOp>> _pendingLeafOps;
    private readonly object _pendingOpsLock;
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
        _pendingLeafOps = new Dictionary<int, List<PendingLeafOp>>();
        _pendingOpsLock = new object();
    }

    public int GetRootPageId()
    {
        return _rootPageId;
    }

    #region Pending Ops Helpers

    private void AddPendingOp(int pageId, PendingLeafOp op)
    {
        lock (_pendingOpsLock)
        {
            if (!_pendingLeafOps.TryGetValue(pageId, out var ops))
            {
                ops = new List<PendingLeafOp>();
                _pendingLeafOps[pageId] = ops;
            }
            ops.Add(op);
        }
    }

    private int GetPendingNetCount(int pageId)
    {
        int net = 0;
        lock (_pendingOpsLock)
        {
            if (_pendingLeafOps.TryGetValue(pageId, out var ops))
            {
                foreach (var op in ops)
                {
                    if (op.OpType == LeafOpType.Insert)
                    {
                        net++;
                    }
                    else if (op.OpType == LeafOpType.Delete)
                    {
                        net--;
                    }
                }
            }
        }
        return net;
    }

    private List<PendingLeafOp> GetPendingOpsSnapshot(int pageId)
    {
        List<PendingLeafOp> result = null;
        lock (_pendingOpsLock)
        {
            if (_pendingLeafOps.TryGetValue(pageId, out var ops) && ops.Count > 0)
            {
                result = new List<PendingLeafOp>(ops);
            }
        }
        return result;
    }

    private DocumentLocation? ApplyPendingOpsForKey(int pageId, int key, DocumentLocation? physicalResult)
    {
        List<PendingLeafOp> ops = GetPendingOpsSnapshot(pageId);
        DocumentLocation? result = physicalResult;
        if (ops != null)
        {
            foreach (var op in ops)
            {
                if (op.Key == key)
                {
                    if (op.OpType == LeafOpType.Insert || op.OpType == LeafOpType.Update)
                    {
                        result = op.Location;
                    }
                    else if (op.OpType == LeafOpType.Delete)
                    {
                        result = null;
                    }
                }
            }
        }
        return result;
    }

    private void ApplyPendingOpsToNode(BTreeNode node, int pageId)
    {
        List<PendingLeafOp> ops = GetPendingOpsSnapshot(pageId);
        if (ops != null)
        {
            foreach (var op in ops)
            {
                if (op.OpType == LeafOpType.Insert)
                {
                    int i = 0;
                    while (i < node.KeyCount && node.Keys[i] < op.Key)
                    {
                        i++;
                    }
                    node.Keys.Insert(i, op.Key);
                    node.LeafValues.Insert(i, op.Location);
                    node.KeyCount++;
                }
                else if (op.OpType == LeafOpType.Delete)
                {
                    for (int i = 0; i < node.KeyCount; i++)
                    {
                        if (node.Keys[i] == op.Key)
                        {
                            node.Keys.RemoveAt(i);
                            node.LeafValues.RemoveAt(i);
                            node.KeyCount--;
                            break;
                        }
                    }
                }
                else if (op.OpType == LeafOpType.Update)
                {
                    for (int i = 0; i < node.KeyCount; i++)
                    {
                        if (node.Keys[i] == op.Key)
                        {
                            node.LeafValues[i] = op.Location;
                            break;
                        }
                    }
                }
            }
        }
    }

    private bool IsNodeLogicallyFull(byte[] buffer, int pageId)
    {
        int physicalCount = BTreeNode.GetKeyCount(buffer);
        if (BTreeNode.IsLeafNode(buffer))
        {
            return physicalCount + GetPendingNetCount(pageId) >= _order - 1;
        }
        return physicalCount >= _order - 1;
    }

    private bool IsSafeForInsertLogical(byte[] buffer, int pageId)
    {
        int physicalCount = BTreeNode.GetKeyCount(buffer);
        if (BTreeNode.IsLeafNode(buffer))
        {
            return physicalCount + GetPendingNetCount(pageId) < _order - 2;
        }
        return physicalCount < _order - 2;
    }

    private void RemapPendingOpsAfterSplit(int oldPageId, int newPageId, int splitKey)
    {
        lock (_pendingOpsLock)
        {
            if (_pendingLeafOps.TryGetValue(oldPageId, out var ops))
            {
                List<PendingLeafOp> leftOps = new List<PendingLeafOp>();
                List<PendingLeafOp> rightOps = new List<PendingLeafOp>();

                foreach (var op in ops)
                {
                    if (op.Key > splitKey)
                    {
                        rightOps.Add(op);
                    }
                    else
                    {
                        leftOps.Add(op);
                    }
                }

                if (leftOps.Count > 0)
                {
                    _pendingLeafOps[oldPageId] = leftOps;
                }
                else
                {
                    _pendingLeafOps.Remove(oldPageId);
                }

                if (rightOps.Count > 0)
                {
                    _pendingLeafOps[newPageId] = rightOps;
                }
            }
        }
    }

    #endregion

    public void Insert(int docId, DocumentLocation location, ulong txId = 0)
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

            if (IsNodeLogicallyFull(buffer, rootPageId))
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
                    InsertNonFull(newRootPageId, buffer, childBuffer, docId, location, heldLocks, txId);
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
                InsertNonFull(rootPageId, buffer, childBuffer, docId, location, heldLocks, txId);
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
    public bool Update(int docId, DocumentLocation newLocation, ulong txId = 0)
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
                    DocumentLocation? physicalLocation = null;
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        physicalLocation = BTreeNode.GetLeafValue(buffer, keyCount, pos);
                    }
                    DocumentLocation? logicalLocation = ApplyPendingOpsForKey(currentPageId, docId, physicalLocation);
                    if (logicalLocation.HasValue)
                    {
                        AddPendingOp(currentPageId, new PendingLeafOp
                        {
                            TxId = txId,
                            OpType = LeafOpType.Update,
                            Key = docId,
                            Location = newLocation,
                            OldLocation = logicalLocation.Value
                        });
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
                ApplyPendingOpsToNode(node, currentPageId);

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

    public bool Delete(int docId, ulong txId = 0)
    {
        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            return DeleteFromNode(rootPageId, docId, txId, rootAlreadyLocked: true);
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
                    ApplyPendingOpsToNode(node, currentPageId);
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

    private bool DeleteFromNode(int pageId, int docId, ulong txId, bool rootAlreadyLocked = false)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;
            if (!rootAlreadyLocked)
            {
                _pageLockManager.AcquireWriteLock(currentPageId);
            }

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);

                int i = 0;
                while (i < keyCount && docId > BTreeNode.GetKey(buffer, i))
                {
                    i++;
                }

                if (BTreeNode.IsLeafNode(buffer))
                {
                    DocumentLocation? physicalLocation = null;
                    if (i < keyCount && BTreeNode.GetKey(buffer, i) == docId)
                    {
                        physicalLocation = BTreeNode.GetLeafValue(buffer, keyCount, i);
                    }
                    DocumentLocation? logicalLocation = ApplyPendingOpsForKey(currentPageId, docId, physicalLocation);
                    if (logicalLocation.HasValue)
                    {
                        AddPendingOp(currentPageId, new PendingLeafOp
                        {
                            TxId = txId,
                            OpType = LeafOpType.Delete,
                            Key = docId,
                            OldLocation = logicalLocation.Value
                        });
                        result = true;
                    }
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, i);
                    _pageLockManager.AcquireWriteLock(childPageId);
                    _pageLockManager.ReleaseWriteLock(currentPageId);
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

    /// <summary>
    /// Rebalances the tree by merging underfull nodes. Must be called when no pending ops exist.
    /// Acquires root write lock internally, blocking all concurrent BTree operations.
    /// Returns true if the root page changed.
    /// </summary>
    internal bool Rebalance()
    {
        _rootLock.EnterWriteLock();
        try
        {
            lock (_pendingOpsLock)
            {
                if (_pendingLeafOps.Count > 0)
                {
                    return false;
                }
            }

            byte[] buffer = BufferPool.Rent(_pageSize);
            BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
            try
            {
                _pageIO.ReadPage(_rootPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);
                RebalanceNode(_rootPageId, node);

                // Collapse root if it's an internal node with no keys
                if (node.NodeType == BTreeNodeType.Internal && node.KeyCount == 0 && node.ChildPageIds.Count == 1)
                {
                    int newRootPageId = node.ChildPageIds[0];
                    _pageManager.DeallocatePage(_rootPageId);
                    _rootPageId = newRootPageId;
                    return true;
                }
            }
            finally
            {
                BufferPool.Return(buffer);
                BTreeNodePool.Return(node);
            }

            return false;
        }
        finally
        {
            _rootLock.ExitWriteLock();
        }
    }

    private void RebalanceNode(int pageId, BTreeNode node)
    {
        if (node.NodeType == BTreeNodeType.Leaf)
        {
            return;
        }

        byte[] childBuffer = BufferPool.Rent(_pageSize);
        BTreeNode child = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        BTreeNode sibling = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            // Visit children. After merges, node.KeyCount may decrease, so re-check bounds.
            int i = 0;
            while (i <= node.KeyCount)
            {
                int childPageId = node.ChildPageIds[i];
                _pageIO.ReadPage(childPageId, childBuffer);
                BTreeNode.DeserializeTo(childBuffer, child, _order);

                // Recurse into child first (bottom-up)
                RebalanceNode(childPageId, child);

                // Re-read child after recursion (it may have changed)
                _pageIO.ReadPage(childPageId, childBuffer);
                BTreeNode.DeserializeTo(childBuffer, child, _order);

                if (!child.IsUnderflow())
                {
                    i++;
                    continue;
                }

                bool handled = false;

                // Try borrow from left sibling
                if (i > 0)
                {
                    int leftPageId = node.ChildPageIds[i - 1];
                    _pageIO.ReadPage(leftPageId, siblingBuffer);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromLeftSibling(pageId, node,
                                              leftPageId, sibling, siblingBuffer,
                                              childPageId, child, childBuffer, i);
                        // Re-read parent since BorrowFromLeftSibling wrote it
                        _pageIO.ReadPage(pageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                        handled = true;
                        i++;
                    }
                }

                // Try borrow from right sibling
                if (!handled && i < node.KeyCount)
                {
                    int rightPageId = node.ChildPageIds[i + 1];
                    _pageIO.ReadPage(rightPageId, siblingBuffer);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromRightSibling(pageId, node,
                                               childPageId, child, childBuffer,
                                               rightPageId, sibling, siblingBuffer, i);
                        _pageIO.ReadPage(pageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                        handled = true;
                        i++;
                    }
                }

                // Merge with a sibling
                if (!handled)
                {
                    if (i > 0)
                    {
                        int leftPageId = node.ChildPageIds[i - 1];
                        _pageIO.ReadPage(leftPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithLeftSibling(pageId, node,
                                             leftPageId, sibling, siblingBuffer,
                                             childPageId, child, i);
                        // Re-read parent after merge modified it
                        _pageIO.ReadPage(pageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                        // Don't increment i — the merge removed a child, so index i now points to next child
                    }
                    else if (i < node.KeyCount)
                    {
                        int rightPageId = node.ChildPageIds[i + 1];
                        _pageIO.ReadPage(rightPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithRightSibling(pageId, node,
                                              childPageId, child, childBuffer,
                                              rightPageId, sibling, siblingBuffer, i);
                        _pageIO.ReadPage(pageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                        // Don't increment i — current absorbed right, right was removed
                    }
                    else
                    {
                        // Single child with no siblings — can't merge, skip
                        i++;
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(childBuffer);
            BufferPool.Return(siblingBuffer);
            BufferPool.Return(parentBuffer);
            BTreeNodePool.Return(child);
            BTreeNodePool.Return(sibling);
        }
    }

    private void BorrowFromLeftSibling(int parentPageId, BTreeNode parent,
                                       int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                       int currentPageId, BTreeNode current, byte[] currentBuffer, int childIndex)
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

        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(parentBuffer);
        }
    }

    private void BorrowFromRightSibling(int parentPageId, BTreeNode parent,
                                        int currentPageId, BTreeNode current, byte[] currentBuffer,
                                        int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                        int childIndex)
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

        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(parentBuffer);
        }
    }

    private void MergeWithLeftSibling(int parentPageId, BTreeNode parent,
                                      int leftSiblingPageId, BTreeNode leftSibling, byte[] leftBuffer,
                                      int currentPageId, BTreeNode current, int childIndex)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int j = 0; j < current.KeyCount; j++)
            {
                leftSibling.Keys.Add(current.Keys[j]);
                leftSibling.LeafValues.Add(current.LeafValues[j]);
            }
            leftSibling.KeyCount += current.KeyCount;

            leftSibling.NextLeaf = current.NextLeaf;
        }
        else
        {
            leftSibling.Keys.Add(parent.Keys[childIndex - 1]);
            leftSibling.KeyCount++;

            for (int j = 0; j < current.KeyCount; j++)
            {
                leftSibling.Keys.Add(current.Keys[j]);
            }
            for (int j = 0; j <= current.KeyCount; j++)
            {
                leftSibling.ChildPageIds.Add(current.ChildPageIds[j]);
            }
            leftSibling.KeyCount += current.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex - 1);
        parent.ChildPageIds.RemoveAt(childIndex);
        parent.KeyCount--;

        leftSibling.SerializeTo(leftBuffer);
        _pageIO.WritePage(leftSiblingPageId, leftBuffer);

        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(parentBuffer);
        }

        _pageManager.DeallocatePage(currentPageId);
    }

    private void MergeWithRightSibling(int parentPageId, BTreeNode parent,
                                       int currentPageId, BTreeNode current, byte[] currentBuffer,
                                       int rightSiblingPageId, BTreeNode rightSibling, byte[] rightBuffer,
                                       int childIndex)
    {
        if (current.NodeType == BTreeNodeType.Leaf)
        {
            for (int j = 0; j < rightSibling.KeyCount; j++)
            {
                current.Keys.Add(rightSibling.Keys[j]);
                current.LeafValues.Add(rightSibling.LeafValues[j]);
            }
            current.KeyCount += rightSibling.KeyCount;

            current.NextLeaf = rightSibling.NextLeaf;
        }
        else
        {
            current.Keys.Add(parent.Keys[childIndex]);
            current.KeyCount++;

            for (int j = 0; j < rightSibling.KeyCount; j++)
            {
                current.Keys.Add(rightSibling.Keys[j]);
            }
            for (int j = 0; j <= rightSibling.KeyCount; j++)
            {
                current.ChildPageIds.Add(rightSibling.ChildPageIds[j]);
            }
            current.KeyCount += rightSibling.KeyCount;
        }

        parent.Keys.RemoveAt(childIndex);
        parent.ChildPageIds.RemoveAt(childIndex + 1);
        parent.KeyCount--;

        current.SerializeTo(currentBuffer);
        _pageIO.WritePage(currentPageId, currentBuffer);

        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            parent.SerializeTo(parentBuffer);
            _pageIO.WritePage(parentPageId, parentBuffer);
        }
        finally
        {
            BufferPool.Return(parentBuffer);
        }

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
                    DocumentLocation? physicalResult = null;
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        physicalResult = BTreeNode.GetLeafValue(buffer, keyCount, pos);
                    }
                    result = ApplyPendingOpsForKey(currentPageId, docId, physicalResult);
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

    private void InsertNonFull(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, LockStack heldLocks, ulong txId)
    {
        int currentPageId = pageId;

        while (true)
        {
            ushort keyCount = BTreeNode.GetKeyCount(buffer);

            if (BTreeNode.IsLeafNode(buffer))
            {
                AddPendingOp(currentPageId, new PendingLeafOp
                {
                    TxId = txId,
                    OpType = LeafOpType.Insert,
                    Key = docId,
                    Location = location
                });
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
                if (IsSafeForInsertLogical(childBuffer, childPageId))
                {
                    heldLocks.ReleaseAll();
                }

                if (IsNodeLogicallyFull(childBuffer, childPageId))
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

            int keyToPromote;

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                // For leaf splits, compute split key from the LOGICAL key set
                // (physical keys + pending inserts - pending deletes)
                List<int> logicalKeys = new List<int>(fullChild.KeyCount);
                HashSet<int> deletedKeys = null;

                // Collect pending ops for this page
                List<PendingLeafOp> pendingOps = GetPendingOpsSnapshot(childPageId);
                if (pendingOps != null)
                {
                    deletedKeys = new HashSet<int>();
                    foreach (var op in pendingOps)
                    {
                        if (op.OpType == LeafOpType.Delete)
                        {
                            deletedKeys.Add(op.Key);
                        }
                    }
                }

                // Add physical keys (excluding pending deletes)
                for (int i = 0; i < fullChild.KeyCount; i++)
                {
                    int key = fullChild.Keys[i];
                    if (deletedKeys == null || !deletedKeys.Contains(key))
                    {
                        logicalKeys.Add(key);
                    }
                }

                // Add pending inserts
                if (pendingOps != null)
                {
                    foreach (var op in pendingOps)
                    {
                        if (op.OpType == LeafOpType.Insert)
                        {
                            logicalKeys.Add(op.Key);
                        }
                    }
                }

                logicalKeys.Sort();
                int logicalMid = logicalKeys.Count / 2;
                keyToPromote = logicalKeys[logicalMid];

                // Redistribute PHYSICAL keys based on split key
                // Left (old page) keeps keys <= keyToPromote, right (new page) gets keys > keyToPromote
                int physicalSplitIndex = fullChild.KeyCount;
                for (int i = 0; i < fullChild.KeyCount; i++)
                {
                    if (fullChild.Keys[i] > keyToPromote)
                    {
                        physicalSplitIndex = i;
                        break;
                    }
                }

                // Move keys > keyToPromote to newChild
                for (int j = physicalSplitIndex; j < fullChild.KeyCount; j++)
                {
                    newChild.Keys.Add(fullChild.Keys[j]);
                    newChild.LeafValues.Add(fullChild.LeafValues[j]);
                    newChild.KeyCount++;
                }

                // Trim left child
                int keysToRemove = fullChild.KeyCount - physicalSplitIndex;
                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(physicalSplitIndex, keysToRemove);
                    fullChild.LeafValues.RemoveRange(physicalSplitIndex, keysToRemove);
                }
                fullChild.KeyCount = (ushort)physicalSplitIndex;

                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                // Internal node split: no pending ops, use physical mid
                int mid = (_order - 1) / 2;
                keyToPromote = fullChild.Keys[mid];

                for (int j = mid + 1; j < fullChild.KeyCount; j++)
                {
                    newChild.Keys.Add(fullChild.Keys[j]);
                    newChild.ChildPageIds.Add(fullChild.ChildPageIds[j]);
                    newChild.KeyCount++;
                }
                newChild.ChildPageIds.Add(fullChild.ChildPageIds[fullChild.KeyCount]);

                int originalKeyCount = fullChild.KeyCount;
                fullChild.KeyCount = (ushort)mid;
                int keysToRemove = originalKeyCount - mid;

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid, keysToRemove);
                }

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

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                RemapPendingOpsAfterSplit(childPageId, newChildPageId, keyToPromote);
            }
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

    #region Pending Ops Flush/Abort/Undo

    public List<FlushedOp> FlushPendingOps(ulong txId)
    {
        List<FlushedOp> flushedOps = new List<FlushedOp>();

        // Process pages one at a time. For each page, we acquire the page write lock
        // BEFORE extracting ops from _pendingLeafOps. This ensures the logical count
        // (physical + pending) is never understated: InsertNonFull holds the page write
        // lock while checking fullness, so it cannot observe the reduced pending count
        // before the physical count has been updated.
        // Lock ordering: page write lock → _pendingOpsLock (matches InsertNonFull/SplitChild).
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            while (true)
            {
                // Find the next page with ops for this txId
                int targetPageId = -1;
                lock (_pendingOpsLock)
                {
                    foreach (var kvp in _pendingLeafOps)
                    {
                        foreach (var op in kvp.Value)
                        {
                            if (op.TxId == txId)
                            {
                                targetPageId = kvp.Key;
                                break;
                            }
                        }
                        if (targetPageId >= 0) break;
                    }
                }

                if (targetPageId < 0) break;

                // Acquire page write lock, then extract and flush atomically
                _pageLockManager.AcquireWriteLock(targetPageId);
                try
                {
                    // Extract ops for this txId from this page (ops may have been
                    // remapped to a different page by a concurrent split — if so,
                    // txOps will be empty and we skip the page)
                    List<PendingLeafOp> txOps = null;
                    lock (_pendingOpsLock)
                    {
                        if (_pendingLeafOps.TryGetValue(targetPageId, out var allOps))
                        {
                            List<PendingLeafOp> remainingOps = null;
                            foreach (var op in allOps)
                            {
                                if (op.TxId == txId)
                                {
                                    txOps ??= new List<PendingLeafOp>();
                                    txOps.Add(op);
                                }
                                else
                                {
                                    remainingOps ??= new List<PendingLeafOp>();
                                    remainingOps.Add(op);
                                }
                            }
                            if (txOps != null)
                            {
                                if (remainingOps != null)
                                {
                                    _pendingLeafOps[targetPageId] = remainingOps;
                                }
                                else
                                {
                                    _pendingLeafOps.Remove(targetPageId);
                                }
                            }
                        }
                    }

                    if (txOps != null && txOps.Count > 0)
                    {
                        _pageIO.ReadPage(targetPageId, buffer);
                        BTreeNode.DeserializeTo(buffer, node, _order);

                        foreach (var op in txOps)
                        {
                            if (op.OpType == LeafOpType.Insert)
                            {
                                int i = 0;
                                while (i < node.KeyCount && node.Keys[i] < op.Key)
                                {
                                    i++;
                                }
                                node.Keys.Insert(i, op.Key);
                                node.LeafValues.Insert(i, op.Location);
                                node.KeyCount++;
                                flushedOps.Add(new FlushedOp { Type = LeafOpType.Insert, Key = op.Key });
                            }
                            else if (op.OpType == LeafOpType.Delete)
                            {
                                for (int i = 0; i < node.KeyCount; i++)
                                {
                                    if (node.Keys[i] == op.Key)
                                    {
                                        node.Keys.RemoveAt(i);
                                        node.LeafValues.RemoveAt(i);
                                        node.KeyCount--;
                                        flushedOps.Add(new FlushedOp { Type = LeafOpType.Delete, Key = op.Key, OldLocation = op.OldLocation });
                                        break;
                                    }
                                }
                            }
                            else if (op.OpType == LeafOpType.Update)
                            {
                                for (int i = 0; i < node.KeyCount; i++)
                                {
                                    if (node.Keys[i] == op.Key)
                                    {
                                        node.LeafValues[i] = op.Location;
                                        flushedOps.Add(new FlushedOp { Type = LeafOpType.Update, Key = op.Key, OldLocation = op.OldLocation });
                                        break;
                                    }
                                }
                            }
                        }

                        node.SerializeTo(buffer);
                        _pageIO.WritePage(targetPageId, buffer);
                    }
                }
                finally
                {
                    _pageLockManager.ReleaseWriteLock(targetPageId);
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return flushedOps;
    }

    public void AbortPendingOps(ulong txId)
    {
        lock (_pendingOpsLock)
        {
            List<int> keysToProcess = new List<int>(_pendingLeafOps.Keys);
            foreach (int pageId in keysToProcess)
            {
                var ops = _pendingLeafOps[pageId];
                ops.RemoveAll(op => op.TxId == txId);
                if (ops.Count == 0)
                {
                    _pendingLeafOps.Remove(pageId);
                }
            }
        }
    }

    public void UndoPendingOps(List<FlushedOp> flushedOps)
    {
        for (int i = flushedOps.Count - 1; i >= 0; i--)
        {
            FlushedOp op = flushedOps[i];
            if (op.Type == LeafOpType.Insert)
            {
                DeleteDirect(op.Key);
            }
            else if (op.Type == LeafOpType.Delete)
            {
                InsertDirect(op.Key, op.OldLocation);
            }
            else if (op.Type == LeafOpType.Update)
            {
                UpdateDirect(op.Key, op.OldLocation);
            }
        }
    }

    #endregion

    #region Direct Write Methods (for undo operations)

    internal void InsertDirect(int docId, DocumentLocation location)
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

                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    _pageIO.ReadPage(newRootPageId, buffer);
                    InsertNonFullDirect(newRootPageId, buffer, childBuffer, docId, location, heldLocks);
                }
                finally
                {
                    BTreeNodePool.Return(root);
                }
            }
            else
            {
                heldLocks.Push(rootPageId);
                InsertNonFullDirect(rootPageId, buffer, childBuffer, docId, location, heldLocks);
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

    private void InsertNonFullDirect(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, LockStack heldLocks)
    {
        int currentPageId = pageId;

        while (true)
        {
            ushort keyCount = BTreeNode.GetKeyCount(buffer);

            if (BTreeNode.IsLeafNode(buffer))
            {
                BTreeNode.InsertIntoLeaf(buffer, keyCount, docId, location, _order);
                _pageIO.WritePage(currentPageId, buffer);
                break;
            }
            else
            {
                int childIndex = BTreeNode.FindChildIndex(buffer, keyCount, docId);
                int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                _pageLockManager.AcquireWriteLock(childPageId);
                _pageIO.ReadPage(childPageId, childBuffer);

                if (BTreeNode.IsSafeForInsert(childBuffer, _order))
                {
                    heldLocks.ReleaseAll();
                }

                if (BTreeNode.IsNodeFull(childBuffer, _order))
                {
                    SplitChild(currentPageId, childIndex, childPageId);

                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = BTreeNode.GetKeyCount(buffer);

                    if (docId > BTreeNode.GetKey(buffer, childIndex))
                    {
                        childIndex++;
                        int newChildPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);
                        _pageLockManager.AcquireWriteLock(newChildPageId);
                        _pageLockManager.ReleaseWriteLock(childPageId);
                        childPageId = newChildPageId;
                    }

                    _pageIO.ReadPage(childPageId, childBuffer);
                }

                heldLocks.Push(childPageId);

                byte[] temp = buffer;
                buffer = childBuffer;
                childBuffer = temp;
                currentPageId = childPageId;
            }
        }
    }

    internal bool DeleteDirect(int docId)
    {
        bool result = false;

        _rootLock.EnterWriteLock();
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(currentPageId);

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);
                int pos = BTreeNode.FindKeyPosition(buffer, keyCount, docId);

                if (BTreeNode.IsLeafNode(buffer))
                {
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
                        try
                        {
                            BTreeNode.DeserializeTo(buffer, node, _order);
                            node.Keys.RemoveAt(pos);
                            node.LeafValues.RemoveAt(pos);
                            node.KeyCount--;
                            node.SerializeTo(buffer);
                            _pageIO.WritePage(currentPageId, buffer);
                        }
                        finally
                        {
                            BTreeNodePool.Return(node);
                        }
                        result = true;
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

        return result;
    }

    internal bool UpdateDirect(int docId, DocumentLocation newLocation)
    {
        bool updated = false;

        _rootLock.EnterWriteLock();
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(currentPageId);

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

    #endregion

    #region Async Methods

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
                await _pageIO.ReadPageAsync(currentPageId, buffer, null, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(buffer, node, _order);
                ApplyPendingOpsToNode(node, currentPageId);

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
            await _pageIO.ReadPageAsync(currentPageId, buffer, null, cancellationToken).ConfigureAwait(false);
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

    public async Task InsertAsync(int docId, DocumentLocation location, ulong txId = 0, CancellationToken cancellationToken = default)
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
            await _pageIO.ReadPageAsync(rootPageId, buffer, null, cancellationToken).ConfigureAwait(false);

            if (IsNodeLogicallyFull(buffer, rootPageId))
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
                    await _pageIO.WritePageAsync(newRootPageId, buffer, null, cancellationToken).ConfigureAwait(false);

                    await SplitChildAsync(newRootPageId, 0, rootPageId, cancellationToken).ConfigureAwait(false);
                    _rootPageId = newRootPageId;

                    // Release rootPageId lock - InsertNonFullAsync will acquire child locks as needed
                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    // Re-read the new root for InsertNonFullAsync
                    await _pageIO.ReadPageAsync(newRootPageId, buffer, null, cancellationToken).ConfigureAwait(false);
                    await InsertNonFullAsync(newRootPageId, buffer, childBuffer, docId, location, heldLocks, txId, cancellationToken).ConfigureAwait(false);
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
                await InsertNonFullAsync(rootPageId, buffer, childBuffer, docId, location, heldLocks, txId, cancellationToken).ConfigureAwait(false);
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

    public async Task<bool> DeleteAsync(int docId, ulong txId = 0, CancellationToken cancellationToken = default)
    {
        await _rootLock.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int rootPageId = _rootPageId;
            await _pageLockManager.AcquireWriteLockAsync(rootPageId, cancellationToken).ConfigureAwait(false);
            return await DeleteFromNodeAsync(rootPageId, docId, txId, cancellationToken, rootAlreadyLocked: true).ConfigureAwait(false);
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
                await _pageIO.ReadPageAsync(currentPageId, buffer, null, cancellationToken).ConfigureAwait(false);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);
                int pos = BTreeNode.FindKeyPosition(buffer, keyCount, docId);

                if (BTreeNode.IsLeafNode(buffer))
                {
                    DocumentLocation? physicalResult = null;
                    if (pos < keyCount && BTreeNode.GetKey(buffer, pos) == docId)
                    {
                        physicalResult = BTreeNode.GetLeafValue(buffer, keyCount, pos);
                    }
                    result = ApplyPendingOpsForKey(currentPageId, docId, physicalResult);
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

    private async Task InsertNonFullAsync(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, LockStack heldLocks, ulong txId, CancellationToken cancellationToken)
    {
        int currentPageId = pageId;

        while (true)
        {
            ushort keyCount = BTreeNode.GetKeyCount(buffer);

            if (BTreeNode.IsLeafNode(buffer))
            {
                AddPendingOp(currentPageId, new PendingLeafOp
                {
                    TxId = txId,
                    OpType = LeafOpType.Insert,
                    Key = docId,
                    Location = location
                });
                break;
            }
            else
            {
                // Find child to descend into
                int childIndex = BTreeNode.FindChildIndex(buffer, keyCount, docId);
                int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                // Acquire lock on child before reading
                await _pageLockManager.AcquireWriteLockAsync(childPageId, cancellationToken).ConfigureAwait(false);
                await _pageIO.ReadPageAsync(childPageId, childBuffer, null, cancellationToken).ConfigureAwait(false);

                // Safe node optimization: if child won't split, release all ancestor locks
                if (IsSafeForInsertLogical(childBuffer, childPageId))
                {
                    heldLocks.ReleaseAll();
                }

                if (IsNodeLogicallyFull(childBuffer, childPageId))
                {
                    // Need to split child - we hold locks on parent and child
                    await SplitChildAsync(currentPageId, childIndex, childPageId, cancellationToken).ConfigureAwait(false);

                    // Re-read current node after split to get updated key
                    await _pageIO.ReadPageAsync(currentPageId, buffer, null, cancellationToken).ConfigureAwait(false);
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
                    await _pageIO.ReadPageAsync(childPageId, childBuffer, null, cancellationToken).ConfigureAwait(false);
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
            await _pageIO.ReadPageAsync(childPageId, childBuffer, null, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(childBuffer, fullChild, _order);

            await _pageIO.ReadPageAsync(parentPageId, parentBuffer, null, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(parentBuffer, parent, _order);

            newChildPageId = _pageManager.AllocatePage();
            await _pageLockManager.AcquireWriteLockAsync(newChildPageId, cancellationToken).ConfigureAwait(false);
            newChild = BTreeNodePool.Rent(_pageSize, _order, fullChild.NodeType);

            int keyToPromote;

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                // For leaf splits, compute split key from the LOGICAL key set
                List<int> logicalKeys = new List<int>(fullChild.KeyCount);
                HashSet<int> deletedKeys = null;

                List<PendingLeafOp> pendingOps = GetPendingOpsSnapshot(childPageId);
                if (pendingOps != null)
                {
                    deletedKeys = new HashSet<int>();
                    foreach (var op in pendingOps)
                    {
                        if (op.OpType == LeafOpType.Delete)
                        {
                            deletedKeys.Add(op.Key);
                        }
                    }
                }

                for (int i = 0; i < fullChild.KeyCount; i++)
                {
                    int key = fullChild.Keys[i];
                    if (deletedKeys == null || !deletedKeys.Contains(key))
                    {
                        logicalKeys.Add(key);
                    }
                }

                if (pendingOps != null)
                {
                    foreach (var op in pendingOps)
                    {
                        if (op.OpType == LeafOpType.Insert)
                        {
                            logicalKeys.Add(op.Key);
                        }
                    }
                }

                logicalKeys.Sort();
                int logicalMid = logicalKeys.Count / 2;
                keyToPromote = logicalKeys[logicalMid];

                int physicalSplitIndex = fullChild.KeyCount;
                for (int i = 0; i < fullChild.KeyCount; i++)
                {
                    if (fullChild.Keys[i] > keyToPromote)
                    {
                        physicalSplitIndex = i;
                        break;
                    }
                }

                for (int j = physicalSplitIndex; j < fullChild.KeyCount; j++)
                {
                    newChild.Keys.Add(fullChild.Keys[j]);
                    newChild.LeafValues.Add(fullChild.LeafValues[j]);
                    newChild.KeyCount++;
                }

                int keysToRemove = fullChild.KeyCount - physicalSplitIndex;
                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(physicalSplitIndex, keysToRemove);
                    fullChild.LeafValues.RemoveRange(physicalSplitIndex, keysToRemove);
                }
                fullChild.KeyCount = (ushort)physicalSplitIndex;

                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                int mid = (_order - 1) / 2;
                keyToPromote = fullChild.Keys[mid];

                for (int j = mid + 1; j < fullChild.KeyCount; j++)
                {
                    newChild.Keys.Add(fullChild.Keys[j]);
                    newChild.ChildPageIds.Add(fullChild.ChildPageIds[j]);
                    newChild.KeyCount++;
                }
                newChild.ChildPageIds.Add(fullChild.ChildPageIds[fullChild.KeyCount]);

                int originalKeyCount = fullChild.KeyCount;
                fullChild.KeyCount = (ushort)mid;
                int keysToRemove = originalKeyCount - mid;

                if (keysToRemove > 0)
                {
                    fullChild.Keys.RemoveRange(mid, keysToRemove);
                }

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
            await _pageIO.WritePageAsync(childPageId, childBuffer, null, cancellationToken).ConfigureAwait(false);

            newChild.SerializeTo(childBuffer);
            await _pageIO.WritePageAsync(newChildPageId, childBuffer, null, cancellationToken).ConfigureAwait(false);

            parent.SerializeTo(parentBuffer);
            await _pageIO.WritePageAsync(parentPageId, parentBuffer, null, cancellationToken).ConfigureAwait(false);

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                RemapPendingOpsAfterSplit(childPageId, newChildPageId, keyToPromote);
            }
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

    private async Task<bool> DeleteFromNodeAsync(int pageId, int docId, ulong txId, CancellationToken cancellationToken, bool rootAlreadyLocked = false)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;
            if (!rootAlreadyLocked)
            {
                await _pageLockManager.AcquireWriteLockAsync(currentPageId, cancellationToken).ConfigureAwait(false);
            }

            while (currentPageId != 0)
            {
                await _pageIO.ReadPageAsync(currentPageId, buffer, null, cancellationToken).ConfigureAwait(false);
                ushort keyCount = BTreeNode.GetKeyCount(buffer);

                int i = 0;
                while (i < keyCount && docId > BTreeNode.GetKey(buffer, i))
                {
                    i++;
                }

                if (BTreeNode.IsLeafNode(buffer))
                {
                    DocumentLocation? physicalLocation = null;
                    if (i < keyCount && BTreeNode.GetKey(buffer, i) == docId)
                    {
                        physicalLocation = BTreeNode.GetLeafValue(buffer, keyCount, i);
                    }
                    DocumentLocation? logicalLocation = ApplyPendingOpsForKey(currentPageId, docId, physicalLocation);
                    if (logicalLocation.HasValue)
                    {
                        AddPendingOp(currentPageId, new PendingLeafOp
                        {
                            TxId = txId,
                            OpType = LeafOpType.Delete,
                            Key = docId,
                            OldLocation = logicalLocation.Value
                        });
                        result = true;
                    }
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = BTreeNode.GetChildPageId(buffer, keyCount, i);
                    await _pageLockManager.AcquireWriteLockAsync(childPageId, cancellationToken).ConfigureAwait(false);
                    _pageLockManager.ReleaseWriteLock(currentPageId);
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

    #endregion
}
