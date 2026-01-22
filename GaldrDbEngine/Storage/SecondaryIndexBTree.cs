using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

internal class SecondaryIndexBTree
{
    private const int MAX_TREE_DEPTH = 32;

    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly int _pageSize;
    private readonly int _maxKeys;
    private int _rootPageId;

    public SecondaryIndexBTree(IPageIO pageIO, PageManager pageManager, int rootPageId, int pageSize, int maxKeys)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _rootPageId = rootPageId;
        _pageSize = pageSize;
        _maxKeys = maxKeys;
    }

    public int GetRootPageId()
    {
        return _rootPageId;
    }

    public void Insert(byte[] key, DocumentLocation location)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(_rootPageId, buffer);

            if (SecondaryIndexNode.IsNodeFull(buffer, _maxKeys, _pageSize))
            {
                // Root is full - need to split it first
                SecondaryIndexNode newRoot = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Internal);
                try
                {
                    int newRootPageId = _pageManager.AllocatePage();
                    newRoot.ChildPageIds.Add(_rootPageId);

                    newRoot.SerializeTo(buffer);
                    _pageIO.WritePage(newRootPageId, buffer);

                    SplitChild(newRootPageId, 0, _rootPageId);
                    _rootPageId = newRootPageId;

                    // Re-read the new root for InsertNonFull
                    _pageIO.ReadPage(_rootPageId, buffer);
                    InsertNonFull(_rootPageId, buffer, childBuffer, key, location);
                }
                finally
                {
                    SecondaryIndexNodePool.Return(newRoot);
                }
            }
            else
            {
                // Pass the already-read buffer to InsertNonFull
                InsertNonFull(_rootPageId, buffer, childBuffer, key, location);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
        }
    }

    public DocumentLocation? Search(byte[] key)
    {
        return SearchNode(_rootPageId, key);
    }

    public List<DocumentLocation> SearchRange(byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd)
    {
        List<DocumentLocation> results = new List<DocumentLocation>();
        SearchRangeNode(_rootPageId, startKey, endKey, includeStart, includeEnd, results);
        return results;
    }

    public List<DocumentLocation> SearchByFieldValue(byte[] fieldValueKey)
    {
        List<DocumentLocation> results = new List<DocumentLocation>();
        SearchByFieldValueNode(_rootPageId, fieldValueKey, results);
        return results;
    }

    public bool Delete(byte[] key)
    {
        return DeleteFromNode(_rootPageId, key);
    }

    public List<int> CollectAllPageIds()
    {
        List<int> pageIds = new List<int>();
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        Stack<int> pageStack = new Stack<int>();
        pageStack.Push(_rootPageId);

        try
        {
            while (pageStack.Count > 0)
            {
                int currentPageId = pageStack.Pop();
                pageIds.Add(currentPageId);

                _pageIO.ReadPage(currentPageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);

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
            SecondaryIndexNodePool.Return(node);
        }

        return pageIds;
    }

    private DocumentLocation? SearchNode(int pageId, byte[] key)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            ReadOnlySpan<byte> keySpan = key.AsSpan();
            int i = 0;
            while (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) > 0)
            {
                i++;
            }

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                if (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) == 0)
                {
                    result = node.LeafValues[i];
                }
            }
            else
            {
                result = SearchNode(node.ChildPageIds[i], key);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            SecondaryIndexNodePool.Return(node);
        }

        return result;
    }

    private void SearchByFieldValueNode(int pageId, byte[] fieldValueKey, List<DocumentLocation> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                for (int i = 0; i < node.KeyCount; i++)
                {
                    KeyBuffer nodeKey = node.Keys[i];
                    if (nodeKey.StartsWith(fieldValueKey))
                    {
                        results.Add(node.LeafValues[i]);
                    }
                    else if (KeyBuffer.Compare(fieldValueKey.AsSpan(), nodeKey) < 0 && !nodeKey.StartsWith(fieldValueKey))
                    {
                        break;
                    }
                }
            }
            else
            {
                int i = 0;
                while (i < node.KeyCount && KeyBuffer.Compare(fieldValueKey.AsSpan(), node.Keys[i]) > 0)
                {
                    i++;
                }

                for (int j = i; j <= node.KeyCount && j < node.ChildPageIds.Count; j++)
                {
                    SearchByFieldValueNode(node.ChildPageIds[j], fieldValueKey, results);

                    if (j < node.KeyCount && !node.Keys[j].StartsWith(fieldValueKey) &&
                        KeyBuffer.Compare(fieldValueKey.AsSpan(), node.Keys[j]) < 0)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            SecondaryIndexNodePool.Return(node);
        }
    }

    private void SearchRangeNode(int pageId, byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd, List<DocumentLocation> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            ReadOnlySpan<byte> startSpan = startKey.AsSpan();
            ReadOnlySpan<byte> endSpan = endKey.AsSpan();

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                for (int i = 0; i < node.KeyCount; i++)
                {
                    KeyBuffer nodeKey = node.Keys[i];
                    int startCmp = startKey == null ? 1 : KeyBuffer.Compare(startSpan, nodeKey);
                    int endCmp = endKey == null ? -1 : KeyBuffer.Compare(endSpan, nodeKey);

                    bool afterStart = startCmp < 0 || (includeStart && startCmp == 0);
                    bool beforeEnd = endCmp > 0 || (includeEnd && endCmp == 0);

                    if (afterStart && beforeEnd)
                    {
                        results.Add(node.LeafValues[i]);
                    }
                }
            }
            else
            {
                for (int i = 0; i <= node.KeyCount && i < node.ChildPageIds.Count; i++)
                {
                    bool shouldDescend = true;

                    if (i < node.KeyCount && startKey != null)
                    {
                        int cmp = KeyBuffer.Compare(startSpan, node.Keys[i]);
                        if (cmp > 0)
                        {
                            shouldDescend = false;
                        }
                    }

                    if (shouldDescend)
                    {
                        SearchRangeNode(node.ChildPageIds[i], startKey, endKey, includeStart, includeEnd, results);
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            SecondaryIndexNodePool.Return(node);
        }
    }

    private bool DeleteFromNode(int pageId, byte[] key)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        List<DeletePathEntry> path = ListPool<DeletePathEntry>.Rent(MAX_TREE_DEPTH);
        try
        {
            int currentPageId = pageId;
            ReadOnlySpan<byte> keySpan = key.AsSpan();

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);

                int i = 0;
                while (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) > 0)
                {
                    i++;
                }

                if (node.NodeType == BTreeNodeType.Leaf)
                {
                    if (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) == 0)
                    {
                        node.Keys.RemoveAt(i);
                        node.LeafValues.RemoveAt(i);
                        node.KeyCount--;

                        node.SerializeTo(buffer);
                        _pageIO.WritePage(currentPageId, buffer);

                        if (node.IsUnderflow() && currentPageId != _rootPageId)
                        {
                            RebalanceAfterDelete(path, currentPageId, node);
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
            SecondaryIndexNodePool.Return(node);
            ListPool<DeletePathEntry>.Return(path);
        }

        return result;
    }

    private void RebalanceAfterDelete(List<DeletePathEntry> path, int nodePageId, SecondaryIndexNode node)
    {
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode parent = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Internal);
        SecondaryIndexNode sibling = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);

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
                SecondaryIndexNode.DeserializeTo(parentBuffer, parent);

                bool rebalanced = false;

                if (childIndex > 0)
                {
                    int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                    _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                    SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

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
                    SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromRightSibling(parentPageId, parent, parentBuffer,
                                               currentPageId, node,
                                               rightSiblingPageId, sibling, siblingBuffer,
                                               childIndex);

                        if (childIndex == parent.KeyCount - 1 && pathIndex > 0)
                        {
                            PropagateMaxKeyUpdate(path, pathIndex - 1, parent.Keys[parent.KeyCount - 1]);
                        }

                        rebalanced = true;
                    }
                }

                if (!rebalanced)
                {
                    bool merged = false;

                    if (childIndex > 0)
                    {
                        int leftSiblingPageId = parent.ChildPageIds[childIndex - 1];
                        _pageIO.ReadPage(leftSiblingPageId, siblingBuffer);
                        SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                        int separatorKeyLength = parent.Keys[childIndex - 1].Length;
                        if (sibling.CanMergeWith(node, separatorKeyLength))
                        {
                            MergeWithLeftSibling(parentPageId, parent, parentBuffer,
                                                 leftSiblingPageId, sibling, siblingBuffer,
                                                 currentPageId, node, childIndex);

                            currentPageId = parentPageId;
                            _pageIO.ReadPage(parentPageId, parentBuffer);
                            SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                            merged = true;
                        }
                    }

                    if (!merged && childIndex < parent.KeyCount)
                    {
                        int rightSiblingPageId = parent.ChildPageIds[childIndex + 1];
                        _pageIO.ReadPage(rightSiblingPageId, siblingBuffer);
                        SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                        int separatorKeyLength = parent.Keys[childIndex].Length;
                        if (node.CanMergeWith(sibling, separatorKeyLength))
                        {
                            MergeWithRightSibling(parentPageId, parent, parentBuffer,
                                                  currentPageId, node,
                                                  rightSiblingPageId, sibling, siblingBuffer,
                                                  childIndex);

                            currentPageId = parentPageId;
                            _pageIO.ReadPage(parentPageId, parentBuffer);
                            SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                            merged = true;
                        }
                    }

                    if (!merged)
                    {
                        break;
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
            SecondaryIndexNodePool.Return(parent);
            SecondaryIndexNodePool.Return(sibling);
        }
    }

    private void PropagateMaxKeyUpdate(List<DeletePathEntry> path, int startPathIndex, KeyBuffer newMaxKey)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Internal);

        try
        {
            int pathIndex = startPathIndex;
            KeyBuffer currentMaxKey = newMaxKey;

            while (pathIndex >= 0)
            {
                DeletePathEntry entry = path[pathIndex];
                int pageId = entry.PageId;
                int childIndex = entry.ChildIndex;

                _pageIO.ReadPage(pageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);

                if (childIndex < node.KeyCount)
                {
                    node.Keys[childIndex] = currentMaxKey;
                    node.SerializeTo(buffer);
                    _pageIO.WritePage(pageId, buffer);

                    if (childIndex < node.KeyCount - 1)
                    {
                        break;
                    }
                }

                pathIndex--;
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            SecondaryIndexNodePool.Return(node);
        }
    }

    private void BorrowFromLeftSibling(int parentPageId, SecondaryIndexNode parent, byte[] parentBuffer,
                                       int leftSiblingPageId, SecondaryIndexNode leftSibling, byte[] leftBuffer,
                                       int currentPageId, SecondaryIndexNode current, int childIndex)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                KeyBuffer borrowedKey = leftSibling.Keys[leftSibling.KeyCount - 1];
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
                KeyBuffer separatorKey = parent.Keys[childIndex - 1];

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

    private void BorrowFromRightSibling(int parentPageId, SecondaryIndexNode parent, byte[] parentBuffer,
                                        int currentPageId, SecondaryIndexNode current,
                                        int rightSiblingPageId, SecondaryIndexNode rightSibling, byte[] rightBuffer,
                                        int childIndex)
    {
        byte[] currentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            if (current.NodeType == BTreeNodeType.Leaf)
            {
                KeyBuffer borrowedKey = rightSibling.Keys[0];
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
                KeyBuffer separatorKey = parent.Keys[childIndex];

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

    private void MergeWithLeftSibling(int parentPageId, SecondaryIndexNode parent, byte[] parentBuffer,
                                      int leftSiblingPageId, SecondaryIndexNode leftSibling, byte[] leftBuffer,
                                      int currentPageId, SecondaryIndexNode current, int childIndex)
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

    private void MergeWithRightSibling(int parentPageId, SecondaryIndexNode parent, byte[] parentBuffer,
                                       int currentPageId, SecondaryIndexNode current,
                                       int rightSiblingPageId, SecondaryIndexNode rightSibling, byte[] rightBuffer,
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

    private void InsertNonFull(int pageId, byte[] buffer, byte[] childBuffer, byte[] key, DocumentLocation location)
    {
        int currentPageId = pageId;
        ReadOnlySpan<byte> keySpan = key.AsSpan();

        while (true)
        {
            ushort keyCount = SecondaryIndexNode.GetKeyCount(buffer);

            if (SecondaryIndexNode.IsLeafNode(buffer))
            {
                SecondaryIndexNode.InsertIntoLeaf(buffer, keyCount, key, location, _pageSize);
                _pageIO.WritePage(currentPageId, buffer);
                break;
            }
            else
            {
                int childIndex = SecondaryIndexNode.FindKeyPosition(buffer, keyCount, keySpan);
                int childPageId = SecondaryIndexNode.GetChildPageId(buffer, keyCount, childIndex);

                // Read child and check if full
                _pageIO.ReadPage(childPageId, childBuffer);
                if (SecondaryIndexNode.IsNodeFull(childBuffer, _maxKeys, _pageSize))
                {
                    SplitChild(currentPageId, childIndex, childPageId);

                    // Re-read current node after split
                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = SecondaryIndexNode.GetKeyCount(buffer);

                    // Check which child to descend into after split
                    int splitKeyOffset = GetKeyOffsetAtIndex(buffer, childIndex);
                    ushort splitKeyLength = BinaryHelper.ReadUInt16LE(buffer, splitKeyOffset);
                    ReadOnlySpan<byte> splitKey = buffer.AsSpan(splitKeyOffset + 2, splitKeyLength);

                    if (keySpan.SequenceCompareTo(splitKey) > 0)
                    {
                        childIndex++;
                    }
                    childPageId = SecondaryIndexNode.GetChildPageId(buffer, keyCount, childIndex);

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

    private static int GetKeyOffsetAtIndex(byte[] buffer, int index)
    {
        int offset = 10; // OFFSET_KEYS
        for (int i = 0; i < index; i++)
        {
            ushort len = BinaryHelper.ReadUInt16LE(buffer, offset);
            offset += 2 + len;
        }
        return offset;
    }

    private void SplitChild(int parentPageId, int index, int childPageId)
    {
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode fullChild = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode parent = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode newChild = null;
        try
        {
            _pageIO.ReadPage(childPageId, childBuffer);
            SecondaryIndexNode.DeserializeTo(childBuffer, fullChild);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            SecondaryIndexNode.DeserializeTo(parentBuffer, parent);

            int newChildPageId = _pageManager.AllocatePage();
            newChild = SecondaryIndexNodePool.Rent(_pageSize, _maxKeys, fullChild.NodeType);

            int mid = Math.Min(_maxKeys / 2, fullChild.KeyCount / 2);
            if (mid >= fullChild.KeyCount)
            {
                mid = fullChild.KeyCount - 1;
            }
            KeyBuffer keyToPromote = fullChild.Keys[mid];

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
            fullChild.KeyCount = (ushort)(mid + 1);
            int keysToRemove = originalKeyCount - (mid + 1);

            if (keysToRemove > 0)
            {
                fullChild.Keys.RemoveRange(mid + 1, keysToRemove);
            }

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                if (keysToRemove > 0)
                {
                    fullChild.LeafValues.RemoveRange(mid + 1, keysToRemove);
                }
                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                int childPointersToRemove = fullChild.ChildPageIds.Count - (mid + 2);
                if (childPointersToRemove > 0)
                {
                    fullChild.ChildPageIds.RemoveRange(mid + 2, childPointersToRemove);
                }
            }

            parent.ChildPageIds.Insert(index + 1, newChildPageId);
            KeyBuffer promotedKeyCopy = KeyBuffer.FromCopy(keyToPromote.Data, keyToPromote.Offset, keyToPromote.Length);
            parent.Keys.Insert(index, promotedKeyCopy);
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
            SecondaryIndexNodePool.Return(fullChild);
            SecondaryIndexNodePool.Return(parent);
            SecondaryIndexNodePool.Return(newChild);
        }
    }

    public static byte[] CreateCompositeKey(byte[] fieldValue, int docId)
    {
        byte[] docIdBytes = new byte[4];
        docIdBytes[0] = (byte)(docId >> 24);
        docIdBytes[1] = (byte)(docId >> 16);
        docIdBytes[2] = (byte)(docId >> 8);
        docIdBytes[3] = (byte)docId;

        byte[] compositeKey = new byte[fieldValue.Length + 4];
        Array.Copy(fieldValue, 0, compositeKey, 0, fieldValue.Length);
        Array.Copy(docIdBytes, 0, compositeKey, fieldValue.Length, 4);

        return compositeKey;
    }

    public static int CalculateMaxKeys(int pageSize)
    {
        int usableSpace = pageSize - 10;
        int avgKeySize = 64;
        int valueSize = 8;

        int maxKeys = usableSpace / (2 + avgKeySize + valueSize);

        if (maxKeys < 4)
        {
            maxKeys = 4;
        }

        return maxKeys;
    }
}
