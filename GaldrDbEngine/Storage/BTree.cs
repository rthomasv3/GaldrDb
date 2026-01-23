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
    private readonly int _pageSize;
    private readonly int _order;
    private int _rootPageId;

    public BTree(IPageIO pageIO, PageManager pageManager, int rootPageId, int pageSize, int order)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
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
        try
        {
            _pageIO.ReadPage(_rootPageId, buffer);

            if (BTreeNode.IsNodeFull(buffer, _order))
            {
                // Root is full - need to split it first
                BTreeNode root = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
                try
                {
                    BTreeNode.DeserializeTo(buffer, root, _order);

                    int newRootPageId = _pageManager.AllocatePage();
                    newRoot = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
                    newRoot.ChildPageIds.Add(_rootPageId);

                    newRoot.SerializeTo(buffer);
                    _pageIO.WritePage(newRootPageId, buffer);

                    SplitChild(newRootPageId, 0, _rootPageId);
                    _rootPageId = newRootPageId;

                    // Re-read the new root for InsertNonFull
                    _pageIO.ReadPage(_rootPageId, buffer);
                    InsertNonFull(_rootPageId, buffer, childBuffer, docId, location);
                }
                finally
                {
                    BTreeNodePool.Return(root);
                }
            }
            else
            {
                // Pass the already-read buffer to InsertNonFull
                InsertNonFull(_rootPageId, buffer, childBuffer, docId, location);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
            BTreeNodePool.Return(newRoot);
        }
    }

    public DocumentLocation? Search(int docId)
    {
        return SearchNode(_rootPageId, docId);
    }

    public List<BTreeEntry> SearchRange(int startDocId, int endDocId, bool includeStart, bool includeEnd)
    {
        List<BTreeEntry> results = new List<BTreeEntry>();

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int leafPageId = FindLeafForKey(_rootPageId, startDocId, buffer, node);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                bool exceededEnd = ScanLeafForRange(node, startDocId, endDocId, includeStart, includeEnd, results);

                if (exceededEnd || node.NextLeaf == 0)
                {
                    continueScanning = false;
                }
                else
                {
                    currentPageId = node.NextLeaf;
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return results;
    }

    private int FindLeafForKey(int pageId, int targetKey, byte[] buffer, BTreeNode node)
    {
        int currentPageId = pageId;

        while (currentPageId != 0)
        {
            _pageIO.ReadPage(currentPageId, buffer);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                break;
            }

            int i = 0;
            while (i < node.KeyCount && targetKey > node.Keys[i])
            {
                i++;
            }

            currentPageId = node.ChildPageIds[i];
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
        return DeleteFromNode(_rootPageId, docId);
    }

    public List<BTreeEntry> GetAllEntries()
    {
        List<BTreeEntry> entries = new List<BTreeEntry>();
        CollectAllEntries(_rootPageId, entries);
        return entries;
    }

    public void IterateAll(Func<BTreeEntry, bool> visitor)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int leafPageId = FindLeftmostLeaf(_rootPageId, buffer, node);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                for (int i = 0; i < node.KeyCount; i++)
                {
                    BTreeEntry entry = new BTreeEntry(node.Keys[i], node.LeafValues[i]);
                    if (!visitor(entry))
                    {
                        continueScanning = false;
                        break;
                    }
                }

                if (continueScanning)
                {
                    if (node.NextLeaf == 0)
                    {
                        continueScanning = false;
                    }
                    else
                    {
                        currentPageId = node.NextLeaf;
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }
    }

    public async Task IterateAllAsync(Func<BTreeEntry, bool> visitor, CancellationToken cancellationToken = default)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int leafPageId = await FindLeftmostLeafAsync(_rootPageId, buffer, node, cancellationToken).ConfigureAwait(false);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(buffer, node, _order);

                for (int i = 0; i < node.KeyCount; i++)
                {
                    BTreeEntry entry = new BTreeEntry(node.Keys[i], node.LeafValues[i]);
                    if (!visitor(entry))
                    {
                        continueScanning = false;
                        break;
                    }
                }

                if (continueScanning)
                {
                    if (node.NextLeaf == 0)
                    {
                        continueScanning = false;
                    }
                    else
                    {
                        currentPageId = node.NextLeaf;
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }
    }

    private int FindLeftmostLeaf(int pageId, byte[] buffer, BTreeNode node)
    {
        int currentPageId = pageId;

        while (currentPageId != 0)
        {
            _pageIO.ReadPage(currentPageId, buffer);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                break;
            }

            currentPageId = node.ChildPageIds[0];
        }

        return currentPageId;
    }

    private async Task<int> FindLeftmostLeafAsync(int pageId, byte[] buffer, BTreeNode node, CancellationToken cancellationToken)
    {
        int currentPageId = pageId;

        while (currentPageId != 0)
        {
            await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                break;
            }

            currentPageId = node.ChildPageIds[0];
        }

        return currentPageId;
    }

    public List<int> CollectAllPageIds()
    {
        List<int> pageIds = new List<int>();
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        Stack<int> pageStack = new Stack<int>();
        pageStack.Push(_rootPageId);

        try
        {
            while (pageStack.Count > 0)
            {
                int currentPageId = pageStack.Pop();
                pageIds.Add(currentPageId);

                _pageIO.ReadPage(currentPageId, buffer);
                BTreeNode.DeserializeTo(buffer, node, _order);

                if (node.NodeType == BTreeNodeType.Internal)
                {
                    for (int i = node.KeyCount; i >= 0; i--)
                    {
                        pageStack.Push(node.ChildPageIds[i]);
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return pageIds;
    }

    private void CollectAllEntries(int pageId, List<BTreeEntry> entries)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        Stack<int> pageStack = new Stack<int>();
        pageStack.Push(pageId);

        try
        {
            while (pageStack.Count > 0)
            {
                int currentPageId = pageStack.Pop();
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
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }
    }

    private bool DeleteFromNode(int pageId, int docId)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        List<DeletePathEntry> path = ListPool<DeletePathEntry>.Rent(MAX_TREE_DEPTH);
        try
        {
            int currentPageId = pageId;
            int leafPageId = 0;

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
                    path.Add(new DeletePathEntry(currentPageId, i));
                    currentPageId = node.ChildPageIds[i];
                }
            }

            if (result && leafPageId != 0)
            {
                RebalanceAfterDelete(path, leafPageId, node);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
            ListPool<DeletePathEntry>.Return(path);
        }

        return result;
    }

    private void RebalanceAfterDelete(List<DeletePathEntry> path, int nodePageId, BTreeNode node)
    {
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
        BTreeNode sibling = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);

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
                    _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromLeftSibling(parentPageId, parent, parentBuffer,
                                              leftSiblingPageId, sibling, siblingBuffer,
                                              currentPageId, node, childIndex);
                        rebalanced = true;
                    }
                }

                if (!rebalanced && childIndex < parent.KeyCount)
                {
                    int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
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
                }

                if (!rebalanced)
                {
                    if (childIndex > 0)
                    {
                        int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                        _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithLeftSibling(parentPageId, parent, parentBuffer,
                                             leftSiblingPageId, sibling, siblingBuffer,
                                             currentPageId, node, childIndex);

                        currentPageId = parentPageId;
                        _pageIO.ReadPage(parentPageId, parentBuffer);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                    else
                    {
                        int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                        _pageIO.ReadPage(rightSiblingPageId, siblingBuffer);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        MergeWithRightSibling(parentPageId, parent, parentBuffer,
                                              currentPageId, node,
                                              rightSiblingPageId, sibling, siblingBuffer,
                                              childIndex);

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
                int newRootPageId = node.ChildPageIds[0];
                _pageManager.DeallocatePage(_rootPageId);
                _rootPageId = newRootPageId;
            }
        }
        finally
        {
            BufferPool.Return(parentBuffer);
            BufferPool.Return(siblingBuffer);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(sibling);
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

    private DocumentLocation? SearchNode(int pageId, int docId)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;

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
                    break;
                }
                else
                {
                    currentPageId = BTreeNode.GetChildPageId(buffer, keyCount, pos);
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private void InsertNonFull(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location)
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

                // Read child and check if full
                _pageIO.ReadPage(childPageId, childBuffer);
                if (BTreeNode.IsNodeFull(childBuffer, _order))
                {
                    // Need to split child
                    SplitChild(currentPageId, childIndex, childPageId);

                    // Re-read current node after split to get updated key
                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = BTreeNode.GetKeyCount(buffer);

                    // Check which child to descend into after split
                    if (docId > BTreeNode.GetKey(buffer, childIndex))
                    {
                        childIndex++;
                    }
                    childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                    // Read the correct child after split
                    _pageIO.ReadPage(childPageId, childBuffer);
                }

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
        try
        {
            _pageIO.ReadPage(childPageId, childBuffer);
            BTreeNode.DeserializeTo(childBuffer, fullChild, _order);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            BTreeNode.DeserializeTo(parentBuffer, parent, _order);

            int newChildPageId = _pageManager.AllocatePage();
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
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
            BTreeNodePool.Return(fullChild);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(newChild);
        }
    }

    public async Task<DocumentLocation?> SearchAsync(int docId, CancellationToken cancellationToken = default)
    {
        return await SearchNodeAsync(_rootPageId, docId, cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<BTreeEntry>> SearchRangeAsync(int startDocId, int endDocId, bool includeStart, bool includeEnd, CancellationToken cancellationToken = default)
    {
        List<BTreeEntry> results = new List<BTreeEntry>();

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        try
        {
            int leafPageId = await FindLeafForKeyAsync(_rootPageId, startDocId, buffer, node, cancellationToken).ConfigureAwait(false);
            int currentPageId = leafPageId;
            bool continueScanning = currentPageId != 0;

            while (continueScanning)
            {
                await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                BTreeNode.DeserializeTo(buffer, node, _order);

                bool exceededEnd = ScanLeafForRange(node, startDocId, endDocId, includeStart, includeEnd, results);

                if (exceededEnd || node.NextLeaf == 0)
                {
                    continueScanning = false;
                }
                else
                {
                    currentPageId = node.NextLeaf;
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
        }

        return results;
    }

    private async Task<int> FindLeafForKeyAsync(int pageId, int targetKey, byte[] buffer, BTreeNode node, CancellationToken cancellationToken)
    {
        int currentPageId = pageId;

        while (currentPageId != 0)
        {
            await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(buffer, node, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                break;
            }

            int i = 0;
            while (i < node.KeyCount && targetKey > node.Keys[i])
            {
                i++;
            }

            currentPageId = node.ChildPageIds[i];
        }

        return currentPageId;
    }

    public async Task InsertAsync(int docId, DocumentLocation location, CancellationToken cancellationToken = default)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        BTreeNode newRoot = null;
        try
        {
            await _pageIO.ReadPageAsync(_rootPageId, buffer, cancellationToken).ConfigureAwait(false);

            if (BTreeNode.IsNodeFull(buffer, _order))
            {
                // Root is full - need to split it first
                BTreeNode root = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
                try
                {
                    BTreeNode.DeserializeTo(buffer, root, _order);

                    int newRootPageId = _pageManager.AllocatePage();
                    newRoot = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
                    newRoot.ChildPageIds.Add(_rootPageId);

                    newRoot.SerializeTo(buffer);
                    await _pageIO.WritePageAsync(newRootPageId, buffer, cancellationToken).ConfigureAwait(false);

                    await SplitChildAsync(newRootPageId, 0, _rootPageId, cancellationToken).ConfigureAwait(false);
                    _rootPageId = newRootPageId;

                    // Re-read the new root for InsertNonFullAsync
                    await _pageIO.ReadPageAsync(_rootPageId, buffer, cancellationToken).ConfigureAwait(false);
                    await InsertNonFullAsync(_rootPageId, buffer, childBuffer, docId, location, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    BTreeNodePool.Return(root);
                }
            }
            else
            {
                // Pass the already-read buffer to InsertNonFullAsync
                await InsertNonFullAsync(_rootPageId, buffer, childBuffer, docId, location, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
            BTreeNodePool.Return(newRoot);
        }
    }

    public async Task<bool> DeleteAsync(int docId, CancellationToken cancellationToken = default)
    {
        return await DeleteFromNodeAsync(_rootPageId, docId, cancellationToken).ConfigureAwait(false);
    }

    private async Task<DocumentLocation?> SearchNodeAsync(int pageId, int docId, CancellationToken cancellationToken)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            int currentPageId = pageId;

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
                    break;
                }
                else
                {
                    currentPageId = BTreeNode.GetChildPageId(buffer, keyCount, pos);
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private async Task InsertNonFullAsync(int pageId, byte[] buffer, byte[] childBuffer, int docId, DocumentLocation location, CancellationToken cancellationToken)
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

                // Read child and check if full
                await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
                if (BTreeNode.IsNodeFull(childBuffer, _order))
                {
                    // Need to split child
                    await SplitChildAsync(currentPageId, childIndex, childPageId, cancellationToken).ConfigureAwait(false);

                    // Re-read current node after split to get updated key
                    await _pageIO.ReadPageAsync(currentPageId, buffer, cancellationToken).ConfigureAwait(false);
                    keyCount = BTreeNode.GetKeyCount(buffer);

                    // Check which child to descend into after split
                    if (docId > BTreeNode.GetKey(buffer, childIndex))
                    {
                        childIndex++;
                    }
                    childPageId = BTreeNode.GetChildPageId(buffer, keyCount, childIndex);

                    // Read the correct child after split
                    await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
                }

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
        try
        {
            await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(childBuffer, fullChild, _order);

            await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode.DeserializeTo(parentBuffer, parent, _order);

            int newChildPageId = _pageManager.AllocatePage();
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
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
            BTreeNodePool.Return(fullChild);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(newChild);
        }
    }

    private async Task<bool> DeleteFromNodeAsync(int pageId, int docId, CancellationToken cancellationToken)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        BTreeNode node = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);
        List<DeletePathEntry> path = ListPool<DeletePathEntry>.Rent(MAX_TREE_DEPTH);
        try
        {
            int currentPageId = pageId;

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

                        if (node.IsUnderflow() && currentPageId != _rootPageId)
                        {
                            await RebalanceAfterDeleteAsync(path, currentPageId, node, cancellationToken).ConfigureAwait(false);
                        }

                        result = true;
                    }
                    break;
                }
                else
                {
                    path.Add(new DeletePathEntry(currentPageId, i));
                    currentPageId = node.ChildPageIds[i];
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BTreeNodePool.Return(node);
            ListPool<DeletePathEntry>.Return(path);
        }

        return result;
    }

    private async Task RebalanceAfterDeleteAsync(List<DeletePathEntry> path, int nodePageId, BTreeNode node, CancellationToken cancellationToken)
    {
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        BTreeNode parent = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Internal);
        BTreeNode sibling = BTreeNodePool.Rent(_pageSize, _order, BTreeNodeType.Leaf);

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
                    await _pageIO.ReadPageAsync(leftSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                    BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                    if (sibling.CanLendKey())
                    {
                        await BorrowFromLeftSiblingAsync(parentPageId, parent, parentBuffer,
                                              leftSiblingPageId, sibling, siblingBuffer,
                                              currentPageId, node, childIndex, cancellationToken).ConfigureAwait(false);
                        rebalanced = true;
                    }
                }

                if (!rebalanced && childIndex < parent.KeyCount)
                {
                    int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
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
                }

                if (!rebalanced)
                {
                    if (childIndex > 0)
                    {
                        int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                        await _pageIO.ReadPageAsync(leftSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        await MergeWithLeftSiblingAsync(parentPageId, parent, parentBuffer,
                                             leftSiblingPageId, sibling, siblingBuffer,
                                             currentPageId, node, childIndex, cancellationToken).ConfigureAwait(false);

                        currentPageId = parentPageId;
                        await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(parentBuffer, node, _order);
                    }
                    else
                    {
                        int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                        await _pageIO.ReadPageAsync(rightSiblingPageId, siblingBuffer, cancellationToken).ConfigureAwait(false);
                        BTreeNode.DeserializeTo(siblingBuffer, sibling, _order);

                        await MergeWithRightSiblingAsync(parentPageId, parent, parentBuffer,
                                              currentPageId, node,
                                              rightSiblingPageId, sibling, siblingBuffer,
                                              childIndex, cancellationToken).ConfigureAwait(false);

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
                int newRootPageId = node.ChildPageIds[0];
                _pageManager.DeallocatePage(_rootPageId);
                _rootPageId = newRootPageId;
            }
        }
        finally
        {
            BufferPool.Return(parentBuffer);
            BufferPool.Return(siblingBuffer);
            BTreeNodePool.Return(parent);
            BTreeNodePool.Return(sibling);
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
