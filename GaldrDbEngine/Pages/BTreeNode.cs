using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

public class BTreeNode
{
    private const int HEADER_SIZE = 8;

    private readonly int _pageSize;
    private readonly int _order;

    public byte PageType { get; set; }
    public BTreeNodeType NodeType { get; set; }
    public ushort KeyCount { get; set; }
    public int NextLeaf { get; set; }
    public List<int> Keys { get; set; }
    public List<int> ChildPageIds { get; set; }
    public List<DocumentLocation> LeafValues { get; set; }

    public BTreeNode(int pageSize, int order, BTreeNodeType nodeType)
    {
        _pageSize = pageSize;
        _order = order;
        PageType = PageConstants.PAGE_TYPE_BTREE;
        NodeType = nodeType;
        KeyCount = 0;
        NextLeaf = 0;
        // Pre-allocate capacity to avoid list resizing during operations
        // Max keys per node = order - 1
        Keys = new List<int>(order - 1);

        if (nodeType == BTreeNodeType.Internal)
        {
            // Internal nodes have one more child pointer than keys
            ChildPageIds = new List<int>(order);
            LeafValues = null;
        }
        else
        {
            ChildPageIds = null;
            // Leaf nodes have same number of values as keys
            LeafValues = new List<DocumentLocation>(order - 1);
        }
    }

    public bool IsFull()
    {
        return KeyCount >= _order - 1;
    }

    public bool CanInsert()
    {
        return KeyCount < _order - 1;
    }

    public void SerializeTo(byte[] buffer)
    {
        // Clear the buffer first to ensure clean state
        Array.Clear(buffer, 0, _pageSize);

        int offset = 0;

        buffer[offset] = PageType;
        offset += 1;

        buffer[offset] = (byte)NodeType;
        offset += 1;

        BinaryHelper.WriteUInt16LE(buffer, offset, KeyCount);
        offset += 2;

        BinaryHelper.WriteInt32LE(buffer, offset, NextLeaf);
        offset += 4;

        for (int i = 0; i < KeyCount; i++)
        {
            BinaryHelper.WriteInt32LE(buffer, offset, Keys[i]);
            offset += 4;
        }

        if (NodeType == BTreeNodeType.Internal)
        {
            for (int i = 0; i <= KeyCount; i++)
            {
                BinaryHelper.WriteInt32LE(buffer, offset, ChildPageIds[i]);
                offset += 4;
            }
        }
        else
        {
            for (int i = 0; i < KeyCount; i++)
            {
                BinaryHelper.WriteInt32LE(buffer, offset, LeafValues[i].PageId);
                offset += 4;

                BinaryHelper.WriteInt32LE(buffer, offset, LeafValues[i].SlotIndex);
                offset += 4;
            }
        }
    }

    public static void DeserializeTo(byte[] buffer, BTreeNode node, int order)
    {
        int offset = 0;

        node.PageType = buffer[offset];
        offset += 1;

        BTreeNodeType nodeType = (BTreeNodeType)buffer[offset];
        offset += 1;

        // Handle node type change
        if (nodeType != node.NodeType)
        {
            node.NodeType = nodeType;
            if (nodeType == BTreeNodeType.Internal)
            {
                if (node.ChildPageIds == null)
                {
                    node.ChildPageIds = new List<int>(order);
                }
                node.LeafValues = null;
            }
            else
            {
                node.ChildPageIds = null;
                if (node.LeafValues == null)
                {
                    node.LeafValues = new List<DocumentLocation>(order - 1);
                }
            }
        }

        // Clear existing data
        node.Keys.Clear();
        node.ChildPageIds?.Clear();
        node.LeafValues?.Clear();

        node.KeyCount = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        node.NextLeaf = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        for (int i = 0; i < node.KeyCount; i++)
        {
            int key = BinaryHelper.ReadInt32LE(buffer, offset);
            node.Keys.Add(key);
            offset += 4;
        }

        if (nodeType == BTreeNodeType.Internal)
        {
            for (int i = 0; i <= node.KeyCount; i++)
            {
                int childPageId = BinaryHelper.ReadInt32LE(buffer, offset);
                node.ChildPageIds.Add(childPageId);
                offset += 4;
            }
        }
        else
        {
            for (int i = 0; i < node.KeyCount; i++)
            {
                int pageId = BinaryHelper.ReadInt32LE(buffer, offset);
                offset += 4;

                int slotIndex = BinaryHelper.ReadInt32LE(buffer, offset);
                offset += 4;

                DocumentLocation location = new DocumentLocation(pageId, slotIndex);
                node.LeafValues.Add(location);
            }
        }
    }
}
