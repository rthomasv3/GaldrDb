using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;
using GaldrDbEngine.WAL;

namespace GaldrDbEngine.Storage;

internal class SecondaryIndexBTree
{
    private const int MAX_TREE_DEPTH = 32;

    private readonly IPageIO _pageIO;
    private readonly PageManager _pageManager;
    private readonly PageLockManager _pageLockManager;
    private readonly AsyncReaderWriterLock _rootLock;
    private readonly int _pageSize;
    private readonly int _usablePageSize;
    private readonly int _maxKeys;
    private readonly Dictionary<int, List<SecondaryPendingLeafOp>> _pendingLeafOps;
    private readonly object _pendingOpsLock;
    private int _rootPageId;

    public SecondaryIndexBTree(IPageIO pageIO, PageManager pageManager, PageLockManager pageLockManager, int rootPageId, int pageSize, int usablePageSize, int maxKeys)
    {
        _pageIO = pageIO;
        _pageManager = pageManager;
        _pageLockManager = pageLockManager;
        _rootLock = new AsyncReaderWriterLock();
        _rootPageId = rootPageId;
        _pageSize = pageSize;
        _usablePageSize = usablePageSize;
        _maxKeys = maxKeys;
        _pendingLeafOps = new Dictionary<int, List<SecondaryPendingLeafOp>>();
        _pendingOpsLock = new object();
    }

    public int GetRootPageId()
    {
        return _rootPageId;
    }

    #region Pending Ops Helpers

    private void AddPendingOp(int pageId, SecondaryPendingLeafOp op)
    {
        lock (_pendingOpsLock)
        {
            if (!_pendingLeafOps.TryGetValue(pageId, out var ops))
            {
                ops = new List<SecondaryPendingLeafOp>();
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
                    if (op.OpType == LeafOpType.Insert) net++;
                    else if (op.OpType == LeafOpType.Delete) net--;
                }
            }
        }
        return net;
    }

    private List<SecondaryPendingLeafOp> GetPendingOpsSnapshot(int pageId)
    {
        List<SecondaryPendingLeafOp> result = null;
        lock (_pendingOpsLock)
        {
            if (_pendingLeafOps.TryGetValue(pageId, out var ops) && ops.Count > 0)
            {
                result = new List<SecondaryPendingLeafOp>(ops);
            }
        }
        return result;
    }

    private void ApplyPendingOpsToNode(SecondaryIndexNode node, int pageId)
    {
        List<SecondaryPendingLeafOp> ops = GetPendingOpsSnapshot(pageId);
        if (ops != null)
        {
            foreach (var op in ops)
            {
                ReadOnlySpan<byte> opKeySpan = op.Key.AsSpan();
                if (op.OpType == LeafOpType.Insert)
                {
                    int insertIdx = 0;
                    while (insertIdx < node.KeyCount && KeyBuffer.Compare(opKeySpan, node.Keys[insertIdx]) > 0)
                    {
                        insertIdx++;
                    }
                    node.Keys.Insert(insertIdx, KeyBuffer.FromCopy(op.Key, 0, op.Key.Length));
                    node.LeafValues.Insert(insertIdx, op.Location);
                    node.KeyCount++;
                }
                else if (op.OpType == LeafOpType.Delete)
                {
                    for (int i = 0; i < node.KeyCount; i++)
                    {
                        if (KeyBuffer.Compare(opKeySpan, node.Keys[i]) == 0)
                        {
                            node.Keys.RemoveAt(i);
                            node.LeafValues.RemoveAt(i);
                            node.KeyCount--;
                            break;
                        }
                    }
                }
            }
        }
    }

    private bool IsNodeLogicallyFull(byte[] buffer, int pageId)
    {
        int physicalCount = SecondaryIndexNode.GetKeyCount(buffer);
        if (SecondaryIndexNode.IsLeafNode(buffer))
        {
            return physicalCount + GetPendingNetCount(pageId) >= _maxKeys / 2;
        }
        return SecondaryIndexNode.IsNodeFull(buffer, _maxKeys, _usablePageSize);
    }

    private bool IsSafeForInsertLogical(byte[] buffer, int pageId)
    {
        int physicalCount = SecondaryIndexNode.GetKeyCount(buffer);
        if (SecondaryIndexNode.IsLeafNode(buffer))
        {
            return physicalCount + GetPendingNetCount(pageId) < _maxKeys / 2 - 1;
        }
        return SecondaryIndexNode.IsSafeForInsert(buffer, _maxKeys, _usablePageSize);
    }

    private void RemapPendingOpsAfterSplit(int oldPageId, int newPageId, KeyBuffer splitKey)
    {
        lock (_pendingOpsLock)
        {
            if (_pendingLeafOps.TryGetValue(oldPageId, out var ops))
            {
                List<SecondaryPendingLeafOp> leftOps = new List<SecondaryPendingLeafOp>();
                List<SecondaryPendingLeafOp> rightOps = new List<SecondaryPendingLeafOp>();
                ReadOnlySpan<byte> splitKeySpan = splitKey.AsSpan();

                foreach (var op in ops)
                {
                    if (KeyBuffer.Compare(op.Key.AsSpan(), splitKeySpan) > 0)
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

    public void Insert(byte[] key, DocumentLocation location, ulong txId = 0)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        LockStack heldLocks = new LockStack(_pageLockManager, true);

        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            _pageIO.ReadPage(rootPageId, buffer);

            if (IsNodeLogicallyFull(buffer, rootPageId))
            {
                SecondaryIndexNode newRoot = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Internal);
                try
                {
                    int newRootPageId = _pageManager.AllocatePage();
                    _pageLockManager.AcquireWriteLock(newRootPageId);
                    newRoot.ChildPageIds.Add(rootPageId);

                    newRoot.SerializeTo(buffer);
                    _pageIO.WritePage(newRootPageId, buffer);

                    SplitChild(newRootPageId, 0, rootPageId);
                    _rootPageId = newRootPageId;

                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    _pageIO.ReadPage(newRootPageId, buffer);
                    InsertNonFull(newRootPageId, buffer, childBuffer, key, location, heldLocks, txId);
                }
                finally
                {
                    SecondaryIndexNodePool.Return(newRoot);
                }
            }
            else
            {
                heldLocks.Push(rootPageId);
                InsertNonFull(rootPageId, buffer, childBuffer, key, location, heldLocks, txId);
            }
        }
        finally
        {
            _rootLock.ExitWriteLock();
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
        }
    }

    public DocumentLocation? Search(byte[] key)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireReadLock(rootPageId);
            return SearchNode(rootPageId, key, rootAlreadyLocked: true);
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<DocumentLocation> SearchRange(byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<DocumentLocation> results = new List<DocumentLocation>();
            SearchRangeNode(rootPageId, startKey, endKey, includeStart, includeEnd, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<DocumentLocation> SearchByFieldValue(byte[] fieldValueKey)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<DocumentLocation> results = new List<DocumentLocation>();
            SearchByFieldValueNode(rootPageId, fieldValueKey, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<SecondaryIndexEntry> SearchByFieldValueWithDocIds(byte[] fieldValueKey)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<SecondaryIndexEntry> results = new List<SecondaryIndexEntry>();
            SearchByFieldValueNodeWithDocIds(rootPageId, fieldValueKey, false, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<SecondaryIndexEntry> SearchByExactFieldValueWithDocIds(byte[] fieldValueKey)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<SecondaryIndexEntry> results = new List<SecondaryIndexEntry>();
            SearchByFieldValueNodeWithDocIds(rootPageId, fieldValueKey, true, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<SecondaryIndexEntry> SearchRangeWithDocIds(byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<SecondaryIndexEntry> results = new List<SecondaryIndexEntry>();
            SearchRangeNodeWithDocIds(rootPageId, startKey, endKey, includeStart, includeEnd, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public List<SecondaryIndexEntry> SearchPrefixRangeWithDocIds(byte[] startKey, byte[] prefixKey, bool includeStart)
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            List<SecondaryIndexEntry> results = new List<SecondaryIndexEntry>();
            SearchPrefixRangeNodeWithDocIds(rootPageId, startKey, prefixKey, includeStart, results);
            return results;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    public bool Delete(byte[] key, ulong txId = 0)
    {
        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            return DeleteFromNode(rootPageId, key, txId, rootAlreadyLocked: true);
        }
        finally
        {
            _rootLock.ExitWriteLock();
        }
    }

    public List<int> CollectAllPageIds()
    {
        _rootLock.EnterReadLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireReadLock(rootPageId);

            List<int> pageIds = new List<int>();
            byte[] buffer = BufferPool.Rent(_pageSize);
            SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
            Stack<int> pageStack = new Stack<int>();

            try
            {
                pageIds.Add(rootPageId);
                _pageIO.ReadPage(rootPageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);
                if (node.NodeType == BTreeNodeType.Internal)
                {
                    for (int i = node.KeyCount; i >= 0; i--)
                    {
                        pageStack.Push(node.ChildPageIds[i]);
                    }
                }
                _pageLockManager.ReleaseReadLock(rootPageId);

                while (pageStack.Count > 0)
                {
                    int currentPageId = pageStack.Pop();
                    _pageLockManager.AcquireReadLock(currentPageId);
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
                    _pageLockManager.ReleaseReadLock(currentPageId);
                }
            }
            finally
            {
                BufferPool.Return(buffer);
                SecondaryIndexNodePool.Return(node);
            }

            return pageIds;
        }
        finally
        {
            _rootLock.ExitReadLock();
        }
    }

    private DocumentLocation? SearchNode(int pageId, byte[] key, bool rootAlreadyLocked = false)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            int currentPageId = pageId;
            if (!rootAlreadyLocked)
            {
                _pageLockManager.AcquireReadLock(currentPageId);
            }
            ReadOnlySpan<byte> keySpan = key.AsSpan();

            while (currentPageId != 0)
            {
                _pageIO.ReadPage(currentPageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);

                if (node.NodeType == BTreeNodeType.Leaf)
                {
                    ApplyPendingOpsToNode(node, currentPageId);

                    int i = 0;
                    while (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) > 0)
                    {
                        i++;
                    }

                    if (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) == 0)
                    {
                        result = node.LeafValues[i];
                    }
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    break;
                }
                else
                {
                    int i = 0;
                    while (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) > 0)
                    {
                        i++;
                    }

                    int childPageId = node.ChildPageIds[i];
                    _pageLockManager.AcquireReadLock(childPageId);
                    _pageLockManager.ReleaseReadLock(currentPageId);
                    currentPageId = childPageId;
                }
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
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                ApplyPendingOpsToNode(node, pageId);

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
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            ReadOnlySpan<byte> startSpan = startKey.AsSpan();
            ReadOnlySpan<byte> endSpan = endKey.AsSpan();

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                ApplyPendingOpsToNode(node, pageId);

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

    private void SearchByFieldValueNodeWithDocIds(int pageId, byte[] fieldValueKey, bool exactMatch, List<SecondaryIndexEntry> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        int expectedKeyLength = fieldValueKey.Length + 4;

        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                ApplyPendingOpsToNode(node, pageId);

                for (int i = 0; i < node.KeyCount; i++)
                {
                    KeyBuffer nodeKey = node.Keys[i];
                    bool matches = nodeKey.StartsWith(fieldValueKey);

                    if (exactMatch && matches)
                    {
                        matches = nodeKey.Length == expectedKeyLength;
                    }

                    if (matches)
                    {
                        int docId = ExtractDocIdFromKey(nodeKey);
                        results.Add(new SecondaryIndexEntry(docId, node.LeafValues[i]));
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
                    SearchByFieldValueNodeWithDocIds(node.ChildPageIds[j], fieldValueKey, exactMatch, results);

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

    private void SearchRangeNodeWithDocIds(int pageId, byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd, List<SecondaryIndexEntry> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            ReadOnlySpan<byte> startSpan = startKey == null ? ReadOnlySpan<byte>.Empty : startKey.AsSpan();
            ReadOnlySpan<byte> endSpan = endKey == null ? ReadOnlySpan<byte>.Empty : endKey.AsSpan();

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                ApplyPendingOpsToNode(node, pageId);

                for (int i = 0; i < node.KeyCount; i++)
                {
                    KeyBuffer nodeKey = node.Keys[i];
                    bool afterStart;
                    bool beforeEnd;

                    if (startKey == null)
                    {
                        afterStart = true;
                    }
                    else if (nodeKey.StartsWith(startKey))
                    {
                        afterStart = includeStart;
                    }
                    else
                    {
                        int startCmp = KeyBuffer.Compare(startSpan, nodeKey);
                        afterStart = startCmp < 0;
                    }

                    if (endKey == null)
                    {
                        beforeEnd = true;
                    }
                    else if (nodeKey.StartsWith(endKey))
                    {
                        beforeEnd = includeEnd;
                    }
                    else
                    {
                        int endCmp = KeyBuffer.Compare(endSpan, nodeKey);
                        beforeEnd = endCmp > 0;
                    }

                    if (afterStart && beforeEnd)
                    {
                        int docId = ExtractDocIdFromKey(nodeKey);
                        results.Add(new SecondaryIndexEntry(docId, node.LeafValues[i]));
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
                        SearchRangeNodeWithDocIds(node.ChildPageIds[i], startKey, endKey, includeStart, includeEnd, results);
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

    private void SearchPrefixRangeNodeWithDocIds(int pageId, byte[] startKey, byte[] prefixKey, bool includeStart, List<SecondaryIndexEntry> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode.DeserializeTo(buffer, node);

            ReadOnlySpan<byte> startSpan = startKey.AsSpan();

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                ApplyPendingOpsToNode(node, pageId);

                for (int i = 0; i < node.KeyCount; i++)
                {
                    KeyBuffer nodeKey = node.Keys[i];

                    if (!nodeKey.StartsWith(prefixKey))
                    {
                        int cmp = KeyBuffer.Compare(prefixKey.AsSpan(), nodeKey);
                        if (cmp < 0)
                        {
                            break;
                        }
                        continue;
                    }

                    bool afterStart;
                    if (nodeKey.StartsWith(startKey))
                    {
                        afterStart = includeStart;
                    }
                    else
                    {
                        int startCmp = KeyBuffer.Compare(startSpan, nodeKey);
                        afterStart = startCmp < 0;
                    }

                    if (afterStart)
                    {
                        int docId = ExtractDocIdFromKey(nodeKey);
                        results.Add(new SecondaryIndexEntry(docId, node.LeafValues[i]));
                    }
                }
            }
            else
            {
                for (int i = 0; i <= node.KeyCount && i < node.ChildPageIds.Count; i++)
                {
                    bool shouldDescend = true;

                    if (i < node.KeyCount)
                    {
                        int cmp = KeyBuffer.Compare(startSpan, node.Keys[i]);
                        if (cmp > 0)
                        {
                            shouldDescend = false;
                        }
                    }

                    if (shouldDescend)
                    {
                        SearchPrefixRangeNodeWithDocIds(node.ChildPageIds[i], startKey, prefixKey, includeStart, results);
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

    private static int ExtractDocIdFromKey(KeyBuffer key)
    {
        int length = key.Length;
        ReadOnlySpan<byte> data = key.AsSpan();

        int docId = (data[length - 4] << 24) |
                    (data[length - 3] << 16) |
                    (data[length - 2] << 8) |
                    data[length - 1];

        return docId;
    }

    private bool DeleteFromNode(int pageId, byte[] key, ulong txId, bool rootAlreadyLocked = false)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            int currentPageId = pageId;
            ReadOnlySpan<byte> keySpan = key.AsSpan();
            if (!rootAlreadyLocked)
            {
                _pageLockManager.AcquireWriteLock(currentPageId);
            }

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
                    bool keyExists = false;
                    DocumentLocation oldLocation = default;

                    if (i < node.KeyCount && KeyBuffer.Compare(keySpan, node.Keys[i]) == 0)
                    {
                        keyExists = true;
                        oldLocation = node.LeafValues[i];
                    }

                    List<SecondaryPendingLeafOp> pendingOps = GetPendingOpsSnapshot(currentPageId);
                    if (pendingOps != null)
                    {
                        foreach (var op in pendingOps)
                        {
                            if (KeyBuffer.Compare(op.Key.AsSpan(), keySpan) == 0)
                            {
                                if (op.OpType == LeafOpType.Insert)
                                {
                                    keyExists = true;
                                    oldLocation = op.Location;
                                }
                                else if (op.OpType == LeafOpType.Delete)
                                {
                                    keyExists = false;
                                }
                            }
                        }
                    }

                    if (keyExists)
                    {
                        AddPendingOp(currentPageId, new SecondaryPendingLeafOp
                        {
                            TxId = txId,
                            OpType = LeafOpType.Delete,
                            Key = key,
                            OldLocation = oldLocation
                        });
                        result = true;
                    }
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = node.ChildPageIds[i];
                    _pageLockManager.AcquireWriteLock(childPageId);
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = childPageId;
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
            SecondaryIndexNodePool.Return(node);
        }

        return result;
    }

    /// <summary>
    /// Rebalances the tree by merging underfull nodes. Must be called when no pending ops exist.
    /// Acquires root write lock internally, blocking all concurrent SecondaryIndexBTree operations.
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
            SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
            try
            {
                _pageIO.ReadPage(_rootPageId, buffer);
                SecondaryIndexNode.DeserializeTo(buffer, node);
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
                SecondaryIndexNodePool.Return(node);
            }

            return false;
        }
        finally
        {
            _rootLock.ExitWriteLock();
        }
    }

    private void RebalanceNode(int pageId, SecondaryIndexNode node)
    {
        if (node.NodeType == BTreeNodeType.Leaf)
        {
            return;
        }

        byte[] childBuffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode child = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        byte[] siblingBuffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode sibling = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            // Visit children. After merges, node.KeyCount may decrease, so re-check bounds.
            int i = 0;
            while (i <= node.KeyCount)
            {
                int childPageId = node.ChildPageIds[i];
                _pageIO.ReadPage(childPageId, childBuffer);
                SecondaryIndexNode.DeserializeTo(childBuffer, child);

                // Recurse into child first (bottom-up)
                RebalanceNode(childPageId, child);

                // Re-read child after recursion (it may have changed)
                _pageIO.ReadPage(childPageId, childBuffer);
                SecondaryIndexNode.DeserializeTo(childBuffer, child);

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
                    SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromLeftSibling(pageId, node,
                                              leftPageId, sibling, siblingBuffer,
                                              childPageId, child, childBuffer, i);
                        // Re-read parent since BorrowFromLeftSibling wrote it
                        _pageIO.ReadPage(pageId, parentBuffer);
                        SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                        handled = true;
                        i++;
                    }
                }

                // Try borrow from right sibling
                if (!handled && i < node.KeyCount)
                {
                    int rightPageId = node.ChildPageIds[i + 1];
                    _pageIO.ReadPage(rightPageId, siblingBuffer);
                    SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                    if (sibling.CanLendKey())
                    {
                        BorrowFromRightSibling(pageId, node,
                                               childPageId, child, childBuffer,
                                               rightPageId, sibling, siblingBuffer, i);
                        _pageIO.ReadPage(pageId, parentBuffer);
                        SecondaryIndexNode.DeserializeTo(parentBuffer, node);
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
                        SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                        int separatorKeyLength = node.Keys[i - 1].Length;
                        if (sibling.CanMergeWith(child, separatorKeyLength))
                        {
                            MergeWithLeftSibling(pageId, node,
                                                 leftPageId, sibling, siblingBuffer,
                                                 childPageId, child, i);
                            // Re-read parent after merge modified it
                            _pageIO.ReadPage(pageId, parentBuffer);
                            SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                            // Don't increment i -- the merge removed a child, so index i now points to next child
                        }
                        else if (i < node.KeyCount)
                        {
                            int rightPageId = node.ChildPageIds[i + 1];
                            _pageIO.ReadPage(rightPageId, siblingBuffer);
                            SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                            int rightSeparatorKeyLength = node.Keys[i].Length;
                            if (child.CanMergeWith(sibling, rightSeparatorKeyLength))
                            {
                                MergeWithRightSibling(pageId, node,
                                                      childPageId, child, childBuffer,
                                                      rightPageId, sibling, siblingBuffer, i);
                                _pageIO.ReadPage(pageId, parentBuffer);
                                SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                                // Don't increment i -- current absorbed right, right was removed
                            }
                            else
                            {
                                // Can't merge with either sibling, skip
                                i++;
                            }
                        }
                        else
                        {
                            // Can't merge with left (too big) and no right sibling, skip
                            i++;
                        }
                    }
                    else if (i < node.KeyCount)
                    {
                        int rightPageId = node.ChildPageIds[i + 1];
                        _pageIO.ReadPage(rightPageId, siblingBuffer);
                        SecondaryIndexNode.DeserializeTo(siblingBuffer, sibling);

                        int separatorKeyLength = node.Keys[i].Length;
                        if (child.CanMergeWith(sibling, separatorKeyLength))
                        {
                            MergeWithRightSibling(pageId, node,
                                                  childPageId, child, childBuffer,
                                                  rightPageId, sibling, siblingBuffer, i);
                            _pageIO.ReadPage(pageId, parentBuffer);
                            SecondaryIndexNode.DeserializeTo(parentBuffer, node);
                            // Don't increment i -- current absorbed right, right was removed
                        }
                        else
                        {
                            // Can't merge, skip
                            i++;
                        }
                    }
                    else
                    {
                        // Single child with no siblings -- can't merge, skip
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
            SecondaryIndexNodePool.Return(child);
            SecondaryIndexNodePool.Return(sibling);
        }
    }

    private void BorrowFromLeftSibling(int parentPageId, SecondaryIndexNode parent,
                                       int leftSiblingPageId, SecondaryIndexNode leftSibling, byte[] leftBuffer,
                                       int currentPageId, SecondaryIndexNode current, byte[] currentBuffer, int childIndex)
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

    private void BorrowFromRightSibling(int parentPageId, SecondaryIndexNode parent,
                                        int currentPageId, SecondaryIndexNode current, byte[] currentBuffer,
                                        int rightSiblingPageId, SecondaryIndexNode rightSibling, byte[] rightBuffer,
                                        int childIndex)
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

    private void MergeWithLeftSibling(int parentPageId, SecondaryIndexNode parent,
                                      int leftSiblingPageId, SecondaryIndexNode leftSibling, byte[] leftBuffer,
                                      int currentPageId, SecondaryIndexNode current, int childIndex)
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

    private void MergeWithRightSibling(int parentPageId, SecondaryIndexNode parent,
                                       int currentPageId, SecondaryIndexNode current, byte[] currentBuffer,
                                       int rightSiblingPageId, SecondaryIndexNode rightSibling, byte[] rightBuffer,
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

    private void InsertNonFull(int pageId, byte[] buffer, byte[] childBuffer, byte[] key, DocumentLocation location, LockStack heldLocks, ulong txId)
    {
        int currentPageId = pageId;
        ReadOnlySpan<byte> keySpan = key.AsSpan();

        while (true)
        {
            ushort keyCount = SecondaryIndexNode.GetKeyCount(buffer);

            if (SecondaryIndexNode.IsLeafNode(buffer))
            {
                AddPendingOp(currentPageId, new SecondaryPendingLeafOp
                {
                    TxId = txId,
                    OpType = LeafOpType.Insert,
                    Key = key,
                    Location = location
                });
                break;
            }
            else
            {
                int childIndex = SecondaryIndexNode.FindKeyPosition(buffer, keyCount, keySpan);
                int childPageId = SecondaryIndexNode.GetChildPageId(buffer, keyCount, childIndex);

                if (childPageId < 0 || childPageId > 1000000)
                {
                    throw new InvalidOperationException($"InsertNonFull: suspicious childPageId={childPageId} from currentPageId={currentPageId}, keyCount={keyCount}, childIndex={childIndex}, buffer first 32 bytes: {BitConverter.ToString(buffer, 0, 32)}");
                }

                _pageLockManager.AcquireWriteLock(childPageId);
                _pageIO.ReadPage(childPageId, childBuffer);

                if (IsSafeForInsertLogical(childBuffer, childPageId))
                {
                    heldLocks.ReleaseAll();
                }

                if (IsNodeLogicallyFull(childBuffer, childPageId))
                {
                    SplitChild(currentPageId, childIndex, childPageId);

                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = SecondaryIndexNode.GetKeyCount(buffer);

                    int splitKeyOffset = GetKeyOffsetAtIndex(buffer, childIndex);
                    ushort splitKeyLength = BinaryHelper.ReadUInt16LE(buffer, splitKeyOffset);
                    ReadOnlySpan<byte> splitKey = buffer.AsSpan(splitKeyOffset + 2, splitKeyLength);

                    if (keySpan.SequenceCompareTo(splitKey) > 0)
                    {
                        childIndex++;
                        int newChildPageId = SecondaryIndexNode.GetChildPageId(buffer, keyCount, childIndex);
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

    private static int GetKeyOffsetAtIndex(byte[] buffer, int index)
    {
        int offset = 10;
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
        SecondaryIndexNode fullChild = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode parent = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        SecondaryIndexNode newChild = null;
        int newChildPageId = 0;
        try
        {
            _pageIO.ReadPage(childPageId, childBuffer);
            SecondaryIndexNode.DeserializeTo(childBuffer, fullChild);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            SecondaryIndexNode.DeserializeTo(parentBuffer, parent);

            newChildPageId = _pageManager.AllocatePage();
            _pageLockManager.AcquireWriteLock(newChildPageId);
            newChild = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, fullChild.NodeType);

            KeyBuffer keyToPromote;
            int mid;

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                List<byte[]> logicalKeys = new List<byte[]>();

                List<SecondaryPendingLeafOp> pendingOps = GetPendingOpsSnapshot(childPageId);
                HashSet<int> deletedIndices = new HashSet<int>();

                if (pendingOps != null)
                {
                    foreach (var op in pendingOps)
                    {
                        if (op.OpType == LeafOpType.Delete)
                        {
                            for (int i = 0; i < fullChild.KeyCount; i++)
                            {
                                if (KeyBuffer.Compare(op.Key.AsSpan(), fullChild.Keys[i]) == 0)
                                {
                                    deletedIndices.Add(i);
                                    break;
                                }
                            }
                        }
                    }
                }

                for (int i = 0; i < fullChild.KeyCount; i++)
                {
                    if (!deletedIndices.Contains(i))
                    {
                        KeyBuffer kb = fullChild.Keys[i];
                        byte[] keyCopy = new byte[kb.Length];
                        Array.Copy(kb.Data, kb.Offset, keyCopy, 0, kb.Length);
                        logicalKeys.Add(keyCopy);
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

                logicalKeys.Sort((a, b) => KeyBuffer.Compare(a.AsSpan(), b.AsSpan()));

                mid = logicalKeys.Count / 2;
                keyToPromote = KeyBuffer.FromCopy(logicalKeys[mid], 0, logicalKeys[mid].Length);

                ReadOnlySpan<byte> splitKeySpan = keyToPromote.AsSpan();

                for (int j = 0; j < fullChild.KeyCount; j++)
                {
                    if (KeyBuffer.Compare(fullChild.Keys[j].AsSpan(), splitKeySpan) > 0)
                    {
                        newChild.Keys.Add(fullChild.Keys[j]);
                        newChild.LeafValues.Add(fullChild.LeafValues[j]);
                        newChild.KeyCount++;
                    }
                }

                int leftKeyCount = 0;
                for (int j = 0; j < fullChild.KeyCount; j++)
                {
                    if (KeyBuffer.Compare(fullChild.Keys[j].AsSpan(), splitKeySpan) <= 0)
                    {
                        leftKeyCount++;
                    }
                }

                fullChild.Keys.RemoveRange(leftKeyCount, fullChild.KeyCount - leftKeyCount);
                fullChild.LeafValues.RemoveRange(leftKeyCount, fullChild.KeyCount - leftKeyCount);
                fullChild.KeyCount = (ushort)leftKeyCount;

                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;

                RemapPendingOpsAfterSplit(childPageId, newChildPageId, keyToPromote);
            }
            else
            {
                mid = Math.Min(_maxKeys / 2, fullChild.KeyCount / 2);
                if (mid >= fullChild.KeyCount)
                {
                    mid = fullChild.KeyCount - 1;
                }
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
            if (newChildPageId != 0)
            {
                _pageLockManager.ReleaseWriteLock(newChildPageId);
            }
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
            SecondaryIndexNodePool.Return(fullChild);
            SecondaryIndexNodePool.Return(parent);
            SecondaryIndexNodePool.Return(newChild);
        }
    }

    #region Pending Ops Flush/Abort/Undo

    public List<SecondaryFlushedOp> FlushPendingOps(ulong txId)
    {
        List<SecondaryFlushedOp> flushedOps = new List<SecondaryFlushedOp>();

        // Process pages one at a time. For each page, we acquire the page write lock
        // BEFORE extracting ops from _pendingLeafOps. This ensures the logical count
        // (physical + pending) is never understated: InsertNonFull holds the page write
        // lock while checking fullness, so it cannot observe the reduced pending count
        // before the physical count has been updated.
        // Lock ordering: page write lock → _pendingOpsLock (matches InsertNonFull/SplitChild).
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            while (true)
            {
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

                _pageLockManager.AcquireWriteLock(targetPageId);
                try
                {
                    List<SecondaryPendingLeafOp> txOps = null;
                    lock (_pendingOpsLock)
                    {
                        if (_pendingLeafOps.TryGetValue(targetPageId, out var allOps))
                        {
                            List<SecondaryPendingLeafOp> remainingOps = null;
                            foreach (var op in allOps)
                            {
                                if (op.TxId == txId)
                                {
                                    txOps ??= new List<SecondaryPendingLeafOp>();
                                    txOps.Add(op);
                                }
                                else
                                {
                                    remainingOps ??= new List<SecondaryPendingLeafOp>();
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
                        SecondaryIndexNode.DeserializeTo(buffer, node);

                        foreach (var op in txOps)
                        {
                            ReadOnlySpan<byte> opKeySpan = op.Key.AsSpan();
                            if (op.OpType == LeafOpType.Insert)
                            {
                                int insertIdx = 0;
                                while (insertIdx < node.KeyCount && KeyBuffer.Compare(opKeySpan, node.Keys[insertIdx]) > 0)
                                {
                                    insertIdx++;
                                }
                                node.Keys.Insert(insertIdx, KeyBuffer.FromCopy(op.Key, 0, op.Key.Length));
                                node.LeafValues.Insert(insertIdx, op.Location);
                                node.KeyCount++;
                                flushedOps.Add(new SecondaryFlushedOp { Type = LeafOpType.Insert, Key = op.Key });
                            }
                            else if (op.OpType == LeafOpType.Delete)
                            {
                                for (int i = 0; i < node.KeyCount; i++)
                                {
                                    if (KeyBuffer.Compare(opKeySpan, node.Keys[i]) == 0)
                                    {
                                        node.Keys.RemoveAt(i);
                                        node.LeafValues.RemoveAt(i);
                                        node.KeyCount--;
                                        flushedOps.Add(new SecondaryFlushedOp { Type = LeafOpType.Delete, Key = op.Key, OldLocation = op.OldLocation });
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
            SecondaryIndexNodePool.Return(node);
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

    public void UndoPendingOps(List<SecondaryFlushedOp> flushedOps)
    {
        for (int i = flushedOps.Count - 1; i >= 0; i--)
        {
            SecondaryFlushedOp op = flushedOps[i];
            if (op.Type == LeafOpType.Insert)
            {
                DeleteDirect(op.Key);
            }
            else if (op.Type == LeafOpType.Delete)
            {
                InsertDirect(op.Key, op.OldLocation);
            }
        }
    }

    #endregion

    #region Direct Write Methods

    internal void InsertDirect(byte[] key, DocumentLocation location)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        LockStack heldLocks = new LockStack(_pageLockManager, true);

        _rootLock.EnterWriteLock();
        try
        {
            int rootPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(rootPageId);
            _pageIO.ReadPage(rootPageId, buffer);

            if (SecondaryIndexNode.IsNodeFull(buffer, _maxKeys, _usablePageSize))
            {
                SecondaryIndexNode newRoot = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Internal);
                try
                {
                    int newRootPageId = _pageManager.AllocatePage();
                    _pageLockManager.AcquireWriteLock(newRootPageId);
                    newRoot.ChildPageIds.Add(rootPageId);

                    newRoot.SerializeTo(buffer);
                    _pageIO.WritePage(newRootPageId, buffer);

                    SplitChild(newRootPageId, 0, rootPageId);
                    _rootPageId = newRootPageId;

                    _pageLockManager.ReleaseWriteLock(rootPageId);
                    heldLocks.Push(newRootPageId);

                    _pageIO.ReadPage(newRootPageId, buffer);
                    InsertNonFullDirect(newRootPageId, buffer, childBuffer, key, location, heldLocks);
                }
                finally
                {
                    SecondaryIndexNodePool.Return(newRoot);
                }
            }
            else
            {
                heldLocks.Push(rootPageId);
                InsertNonFullDirect(rootPageId, buffer, childBuffer, key, location, heldLocks);
            }
        }
        finally
        {
            _rootLock.ExitWriteLock();
            heldLocks.ReleaseAll();
            BufferPool.Return(buffer);
            BufferPool.Return(childBuffer);
        }
    }

    private void InsertNonFullDirect(int pageId, byte[] buffer, byte[] childBuffer, byte[] key, DocumentLocation location, LockStack heldLocks)
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

                _pageLockManager.AcquireWriteLock(childPageId);
                _pageIO.ReadPage(childPageId, childBuffer);

                if (SecondaryIndexNode.IsSafeForInsert(childBuffer, _maxKeys, _usablePageSize))
                {
                    heldLocks.ReleaseAll();
                }

                if (SecondaryIndexNode.IsNodeFull(childBuffer, _maxKeys, _usablePageSize))
                {
                    SplitChild(currentPageId, childIndex, childPageId);

                    _pageIO.ReadPage(currentPageId, buffer);
                    keyCount = SecondaryIndexNode.GetKeyCount(buffer);

                    int splitKeyOffset = GetKeyOffsetAtIndex(buffer, childIndex);
                    ushort splitKeyLength = BinaryHelper.ReadUInt16LE(buffer, splitKeyOffset);
                    ReadOnlySpan<byte> splitKey = buffer.AsSpan(splitKeyOffset + 2, splitKeyLength);

                    if (keySpan.SequenceCompareTo(splitKey) > 0)
                    {
                        childIndex++;
                        int newChildPageId = SecondaryIndexNode.GetChildPageId(buffer, keyCount, childIndex);
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

    internal bool DeleteDirect(byte[] key)
    {
        bool result = false;

        _rootLock.EnterWriteLock();
        byte[] buffer = BufferPool.Rent(_pageSize);
        SecondaryIndexNode node = SecondaryIndexNodePool.Rent(_usablePageSize, _maxKeys, BTreeNodeType.Leaf);
        try
        {
            int currentPageId = _rootPageId;
            _pageLockManager.AcquireWriteLock(currentPageId);
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
                        result = true;
                    }
                    _pageLockManager.ReleaseWriteLock(currentPageId);
                    currentPageId = 0;
                }
                else
                {
                    int childPageId = node.ChildPageIds[i];
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
            SecondaryIndexNodePool.Return(node);
        }

        return result;
    }

    #endregion

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
