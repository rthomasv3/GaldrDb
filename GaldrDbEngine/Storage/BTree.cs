using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.IO;
using GaldrDbEngine.Pages;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Storage;

public class BTree
{
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
        byte[] rootBuffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(_rootPageId, rootBuffer);
            BTreeNode root = BTreeNode.Deserialize(rootBuffer, _pageSize, _order);

            if (root.IsFull())
            {
                int newRootPageId = _pageManager.AllocatePage();
                BTreeNode newRoot = new BTreeNode(_pageSize, _order, BTreeNodeType.Internal);
                newRoot.ChildPageIds.Add(_rootPageId);

                newRoot.SerializeTo(rootBuffer);
                _pageIO.WritePage(newRootPageId, rootBuffer);

                SplitChild(newRootPageId, 0, _rootPageId);
                _rootPageId = newRootPageId;

                InsertNonFull(newRootPageId, docId, location);
            }
            else
            {
                InsertNonFull(_rootPageId, docId, location);
            }
        }
        finally
        {
            BufferPool.Return(rootBuffer);
        }
    }

    public DocumentLocation Search(int docId)
    {
        return SearchNode(_rootPageId, docId);
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

    private void CollectAllEntries(int pageId, List<BTreeEntry> entries)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                for (int i = 0; i < node.KeyCount; i++)
                {
                    entries.Add(new BTreeEntry(node.Keys[i], node.LeafValues[i]));
                }
            }
            else
            {
                for (int i = 0; i <= node.KeyCount; i++)
                {
                    if (i < node.ChildPageIds.Count)
                    {
                        CollectAllEntries(node.ChildPageIds[i], entries);
                    }
                }
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    private bool DeleteFromNode(int pageId, int docId)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

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
                    _pageIO.WritePage(pageId, buffer);

                    result = true;
                }
            }
            else
            {
                result = DeleteFromNode(node.ChildPageIds[i], docId);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private DocumentLocation SearchNode(int pageId, int docId)
    {
        DocumentLocation result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

            int i = 0;
            while (i < node.KeyCount && docId > node.Keys[i])
            {
                i++;
            }

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                if (i < node.KeyCount && node.Keys[i] == docId)
                {
                    result = node.LeafValues[i];
                }
            }
            else
            {
                result = SearchNode(node.ChildPageIds[i], docId);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private void InsertNonFull(int pageId, int docId, DocumentLocation location)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            _pageIO.ReadPage(pageId, buffer);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

            int i = node.KeyCount - 1;

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                node.Keys.Add(0);
                node.LeafValues.Add(null);

                while (i >= 0 && docId < node.Keys[i])
                {
                    node.Keys[i + 1] = node.Keys[i];
                    node.LeafValues[i + 1] = node.LeafValues[i];
                    i--;
                }

                node.Keys[i + 1] = docId;
                node.LeafValues[i + 1] = location;
                node.KeyCount++;

                node.SerializeTo(buffer);
                _pageIO.WritePage(pageId, buffer);
            }
            else
            {
                while (i >= 0 && docId < node.Keys[i])
                {
                    i--;
                }
                i++;

                byte[] childBuffer = BufferPool.Rent(_pageSize);
                try
                {
                    _pageIO.ReadPage(node.ChildPageIds[i], childBuffer);
                    BTreeNode child = BTreeNode.Deserialize(childBuffer, _pageSize, _order);

                    if (child.IsFull())
                    {
                        SplitChild(pageId, i, node.ChildPageIds[i]);

                        _pageIO.ReadPage(pageId, buffer);
                        node = BTreeNode.Deserialize(buffer, _pageSize, _order);

                        if (docId > node.Keys[i])
                        {
                            i++;
                        }
                    }
                }
                finally
                {
                    BufferPool.Return(childBuffer);
                }

                InsertNonFull(node.ChildPageIds[i], docId, location);
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
            BTreeNode fullChild = BTreeNode.Deserialize(childBuffer, _pageSize, _order);

            _pageIO.ReadPage(parentPageId, parentBuffer);
            BTreeNode parent = BTreeNode.Deserialize(parentBuffer, _pageSize, _order);

            int newChildPageId = _pageManager.AllocatePage();
            BTreeNode newChild = new BTreeNode(_pageSize, _order, fullChild.NodeType);

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

    public async Task<DocumentLocation> SearchAsync(int docId, CancellationToken cancellationToken = default)
    {
        DocumentLocation result = await SearchNodeAsync(_rootPageId, docId, cancellationToken).ConfigureAwait(false);
        return result;
    }

    public async Task InsertAsync(int docId, DocumentLocation location, CancellationToken cancellationToken = default)
    {
        byte[] rootBuffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(_rootPageId, rootBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode root = BTreeNode.Deserialize(rootBuffer, _pageSize, _order);

            if (root.IsFull())
            {
                int newRootPageId = _pageManager.AllocatePage();
                BTreeNode newRoot = new BTreeNode(_pageSize, _order, BTreeNodeType.Internal);
                newRoot.ChildPageIds.Add(_rootPageId);

                newRoot.SerializeTo(rootBuffer);
                await _pageIO.WritePageAsync(newRootPageId, rootBuffer, cancellationToken).ConfigureAwait(false);

                await SplitChildAsync(newRootPageId, 0, _rootPageId, cancellationToken).ConfigureAwait(false);
                _rootPageId = newRootPageId;

                await InsertNonFullAsync(newRootPageId, docId, location, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await InsertNonFullAsync(_rootPageId, docId, location, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            BufferPool.Return(rootBuffer);
        }
    }

    public async Task<bool> DeleteAsync(int docId, CancellationToken cancellationToken = default)
    {
        bool result = await DeleteFromNodeAsync(_rootPageId, docId, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private async Task<DocumentLocation> SearchNodeAsync(int pageId, int docId, CancellationToken cancellationToken)
    {
        DocumentLocation result = null;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

            int i = 0;
            while (i < node.KeyCount && docId > node.Keys[i])
            {
                i++;
            }

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                if (i < node.KeyCount && node.Keys[i] == docId)
                {
                    result = node.LeafValues[i];
                }
            }
            else
            {
                result = await SearchNodeAsync(node.ChildPageIds[i], docId, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }

    private async Task InsertNonFullAsync(int pageId, int docId, DocumentLocation location, CancellationToken cancellationToken)
    {
        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

            int i = node.KeyCount - 1;

            if (node.NodeType == BTreeNodeType.Leaf)
            {
                node.Keys.Add(0);
                node.LeafValues.Add(null);

                while (i >= 0 && docId < node.Keys[i])
                {
                    node.Keys[i + 1] = node.Keys[i];
                    node.LeafValues[i + 1] = node.LeafValues[i];
                    i--;
                }

                node.Keys[i + 1] = docId;
                node.LeafValues[i + 1] = location;
                node.KeyCount++;

                node.SerializeTo(buffer);
                await _pageIO.WritePageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                while (i >= 0 && docId < node.Keys[i])
                {
                    i--;
                }
                i++;

                byte[] childBuffer = BufferPool.Rent(_pageSize);
                try
                {
                    await _pageIO.ReadPageAsync(node.ChildPageIds[i], childBuffer, cancellationToken).ConfigureAwait(false);
                    BTreeNode child = BTreeNode.Deserialize(childBuffer, _pageSize, _order);

                    if (child.IsFull())
                    {
                        await SplitChildAsync(pageId, i, node.ChildPageIds[i], cancellationToken).ConfigureAwait(false);

                        await _pageIO.ReadPageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);
                        node = BTreeNode.Deserialize(buffer, _pageSize, _order);

                        if (docId > node.Keys[i])
                        {
                            i++;
                        }
                    }
                }
                finally
                {
                    BufferPool.Return(childBuffer);
                }

                await InsertNonFullAsync(node.ChildPageIds[i], docId, location, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }
    }

    private async Task SplitChildAsync(int parentPageId, int index, int childPageId, CancellationToken cancellationToken)
    {
        byte[] childBuffer = BufferPool.Rent(_pageSize);
        byte[] parentBuffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(childPageId, childBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode fullChild = BTreeNode.Deserialize(childBuffer, _pageSize, _order);

            await _pageIO.ReadPageAsync(parentPageId, parentBuffer, cancellationToken).ConfigureAwait(false);
            BTreeNode parent = BTreeNode.Deserialize(parentBuffer, _pageSize, _order);

            int newChildPageId = _pageManager.AllocatePage();
            BTreeNode newChild = new BTreeNode(_pageSize, _order, fullChild.NodeType);

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
        }
    }

    private async Task<bool> DeleteFromNodeAsync(int pageId, int docId, CancellationToken cancellationToken)
    {
        bool result = false;

        byte[] buffer = BufferPool.Rent(_pageSize);
        try
        {
            await _pageIO.ReadPageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);
            BTreeNode node = BTreeNode.Deserialize(buffer, _pageSize, _order);

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
                    await _pageIO.WritePageAsync(pageId, buffer, cancellationToken).ConfigureAwait(false);

                    result = true;
                }
            }
            else
            {
                result = await DeleteFromNodeAsync(node.ChildPageIds[i], docId, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            BufferPool.Return(buffer);
        }

        return result;
    }
}
