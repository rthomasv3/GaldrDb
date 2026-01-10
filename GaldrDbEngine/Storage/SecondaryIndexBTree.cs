using System;
using System.Collections.Generic;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

public class SecondaryIndexBTree
{
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
        byte[] rootBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(_rootPageId, rootBuffer);
            SecondaryIndexNode root = SecondaryIndexNode.Deserialize(rootBuffer, _pageSize);

            if (root.IsFull())
            {
                int newRootPageId = _pageManager.AllocatePage();
                SecondaryIndexNode newRoot = new SecondaryIndexNode(_pageSize, _maxKeys, BTreeNodeType.Internal);
                newRoot.ChildPageIds.Add(_rootPageId);

                newRoot.SerializeTo(rootBuffer);
                _pageIO.WritePage(newRootPageId, rootBuffer);

                SplitChild(newRootPageId, 0, _rootPageId);
                _rootPageId = newRootPageId;

                InsertNonFull(newRootPageId, key, location);
            }
            else
            {
                InsertNonFull(_rootPageId, key, location);
            }
        }
        finally
        {
            BufferPool.Return(rootBuffer);
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

    private DocumentLocation? SearchNode(int pageId, byte[] key)
    {
        DocumentLocation? result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

            int i = 0;
            while (i < node.KeyCount && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) > 0)
            {
                i++;
            }

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                if (i < node.KeyCount && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) == 0)
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
        }

        return result;
    }

    private void SearchByFieldValueNode(int pageId, byte[] fieldValueKey, List<DocumentLocation> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                for (int i = 0; i < node.KeyCount; i++)
                {
                    if (KeyStartsWith(node.Keys[i], fieldValueKey))
                    {
                        results.Add(node.LeafValues[i]);
                    }
                    else if (SecondaryIndexNode.CompareKeys(node.Keys[i], fieldValueKey) > 0 &&
                             !KeyStartsWith(node.Keys[i], fieldValueKey))
                    {
                        break;
                    }
                }
            }
            else
            {
                int i = 0;
                while (i < node.KeyCount && SecondaryIndexNode.CompareKeys(fieldValueKey, node.Keys[i]) > 0)
                {
                    i++;
                }

                for (int j = i; j <= node.KeyCount && j < node.ChildPageIds.Count; j++)
                {
                    SearchByFieldValueNode(node.ChildPageIds[j], fieldValueKey, results);

                    if (j < node.KeyCount && !KeyStartsWith(node.Keys[j], fieldValueKey) &&
                        SecondaryIndexNode.CompareKeys(node.Keys[j], fieldValueKey) > 0)
                    {
                        break;
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    private bool KeyStartsWith(byte[] fullKey, byte[] prefix)
    {
        if (fullKey.Length < prefix.Length)
        {
            return false;
        }

        for (int i = 0; i < prefix.Length; i++)
        {
            if (fullKey[i] != prefix[i])
            {
                return false;
            }
        }

        return true;
    }

    private void SearchRangeNode(int pageId, byte[] startKey, byte[] endKey, bool includeStart, bool includeEnd, List<DocumentLocation> results)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                for (int i = 0; i < node.KeyCount; i++)
                {
                    byte[] key = node.Keys[i];
                    int startCmp = startKey == null ? 1 : SecondaryIndexNode.CompareKeys(key, startKey);
                    int endCmp = endKey == null ? -1 : SecondaryIndexNode.CompareKeys(key, endKey);

                    bool afterStart = startCmp > 0 || (includeStart && startCmp == 0);
                    bool beforeEnd = endCmp < 0 || (includeEnd && endCmp == 0);

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
                        int cmp = SecondaryIndexNode.CompareKeys(node.Keys[i], startKey);
                        if (cmp < 0)
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
        }
    }

    private bool DeleteFromNode(int pageId, byte[] key)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

            int i = 0;
            while (i < node.KeyCount && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) > 0)
            {
                i++;
            }

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                if (i < node.KeyCount && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) == 0)
                {
                    node.Keys.RemoveAt(i);
                    node.LeafValues.RemoveAt(i);
                    node.KeyCount--;

                    node.SerializeTo(buffer);
                    _pageIO.WritePage(pageId, buffer);

                    result = true;
                }
            }
            else
            {
                result = DeleteFromNode(node.ChildPageIds[i], key);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private void InsertNonFull(int pageId, byte[] key, DocumentLocation location)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            SecondaryIndexNode node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

            int i = node.KeyCount - 1;

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                node.Keys.Add(null);
                node.LeafValues.Add(default);

                while (i >= 0 && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) < 0)
                {
                    node.Keys[i + 1] = node.Keys[i];
                    node.LeafValues[i + 1] = node.LeafValues[i];
                    i--;
                }

                node.Keys[i + 1] = key;
                node.LeafValues[i + 1] = location;
                node.KeyCount++;

                node.SerializeTo(buffer);
                _pageIO.WritePage(pageId, buffer);
            }
            else
            {
                while (i >= 0 && SecondaryIndexNode.CompareKeys(key, node.Keys[i]) < 0)
                {
                    i--;
                }
                i++;

                byte[] childBuffer = BufferPool.Rent(_pageSize);
                try
                {
                    _pageIO.ReadPage(node.ChildPageIds[i], childBuffer);
                    SecondaryIndexNode child = SecondaryIndexNode.Deserialize(childBuffer, _pageSize);

                    if (child.IsFull())
                    {
                        SplitChild(pageId, i, node.ChildPageIds[i]);

                        _pageIO.ReadPage(pageId, buffer);
                        node = SecondaryIndexNode.Deserialize(buffer, _pageSize);

                        if (SecondaryIndexNode.CompareKeys(key, node.Keys[i]) > 0)
                        {
                            i++;
                        }
                    }
                }
                finally
                {
                    BufferPool.Return(childBuffer);
                }

                InsertNonFull(node.ChildPageIds[i], key, location);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    private void SplitChild(int parentPageId, int index, int childPageId)
    {
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(childPageId, childBuffer);
            SecondaryIndexNode fullChild = SecondaryIndexNode.Deserialize(childBuffer, _pageSize);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            SecondaryIndexNode parent = SecondaryIndexNode.Deserialize(parentBuffer, _pageSize);

            int newChildPageId = _pageManager.AllocatePage();
            SecondaryIndexNode newChild = new SecondaryIndexNode(_pageSize, _maxKeys, fullChild.NodeType);

            int mid = (_maxKeys) / 2;
            byte[] keyToPromote = fullChild.Keys[mid];

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
        int avgKeySize = 20;
        int valueSize = 8;

        int maxKeys = usableSpace / (2 + avgKeySize + valueSize);

        if (maxKeys < 4)
        {
            maxKeys = 4;
        }

        return maxKeys;
    }
}
