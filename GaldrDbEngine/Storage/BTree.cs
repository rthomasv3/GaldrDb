using System;
using GaldrDbCore.IO;
using GaldrDbCore.Pages;
using GaldrDbCore.Utilities;

namespace GaldrDbCore.Storage;

public class BTree
{
    private readonly IPageIO _pageIO;
    private readonly Bitmap _bitmap;
    private readonly int _pageSize;
    private readonly int _order;
    private int _rootPageId;

    public BTree(IPageIO pageIO, Bitmap bitmap, int rootPageId, int pageSize, int order)
    {
        _pageIO = pageIO;
        _bitmap = bitmap;
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
                int newRootPageId = AllocateNewNode();
                BTreeNode newRoot = new BTreeNode(_pageSize, _order, BTreeNodeType.Internal);
                newRoot.ChildPageIds.Add(_rootPageId);

                byte[] newRootBytes = newRoot.Serialize();
                _pageIO.WritePage(newRootPageId, newRootBytes);

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
                if (i < node.KeyCount && node.Keys[i] == docId)
                {
                    i++;
                }

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

                byte[] nodeBytes = node.Serialize();
                _pageIO.WritePage(pageId, nodeBytes);
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

            int newChildPageId = AllocateNewNode();
            BTreeNode newChild = new BTreeNode(_pageSize, _order, fullChild.NodeType);

            int mid = (_order - 1) / 2;

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

            fullChild.KeyCount = (ushort)(mid + 1);
            fullChild.Keys.RemoveRange(mid + 1, fullChild.Keys.Count - mid - 1);

            if (fullChild.NodeType == BTreeNodeType.Leaf)
            {
                fullChild.LeafValues.RemoveRange(mid + 1, fullChild.LeafValues.Count - mid - 1);
                newChild.NextLeaf = fullChild.NextLeaf;
                fullChild.NextLeaf = newChildPageId;
            }
            else
            {
                fullChild.ChildPageIds.RemoveRange(mid + 1, fullChild.ChildPageIds.Count - mid - 1);
            }

            parent.ChildPageIds.Insert(index + 1, newChildPageId);
            parent.Keys.Insert(index, fullChild.Keys[mid]);
            parent.KeyCount++;

            byte[] fullChildBytes = fullChild.Serialize();
            _pageIO.WritePage(childPageId, fullChildBytes);

            byte[] newChildBytes = newChild.Serialize();
            _pageIO.WritePage(newChildPageId, newChildBytes);

            byte[] parentBytes = parent.Serialize();
            _pageIO.WritePage(parentPageId, parentBytes);
        }
        finally
        {
            BufferPool.Return(childBuffer);
            BufferPool.Return(parentBuffer);
        }
    }

    private int AllocateNewNode()
    {
        int pageId = _bitmap.FindFreePage();

        if (pageId == -1)
        {
            throw new InvalidOperationException("No free pages available for B+ tree node");
        }

        _bitmap.AllocatePage(pageId);
        _bitmap.WriteToDisk();

        return pageId;
    }
}
