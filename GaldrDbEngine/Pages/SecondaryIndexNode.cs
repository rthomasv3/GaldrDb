using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

public class SecondaryIndexNode
{
    private const int HEADER_SIZE = 10;

    private readonly int _pageSize;
    private readonly int _maxKeys;

    public byte PageType { get; set; }
    public BTreeNodeType NodeType { get; set; }
    public ushort KeyCount { get; set; }
    public int NextLeaf { get; set; }
    public List<byte[]> Keys { get; set; }
    public List<int> ChildPageIds { get; set; }
    public List<DocumentLocation> LeafValues { get; set; }

    public SecondaryIndexNode(int pageSize, int maxKeys, BTreeNodeType nodeType)
    {
        _pageSize = pageSize;
        _maxKeys = maxKeys;
        PageType = PageConstants.PAGE_TYPE_SECONDARY_INDEX;
        NodeType = nodeType;
        KeyCount = 0;
        NextLeaf = 0;
        Keys = new List<byte[]>();

        if (nodeType == BTreeNodeType.Internal)
        {
            ChildPageIds = new List<int>();
            LeafValues = null;
        }
        else
        {
            ChildPageIds = null;
            LeafValues = new List<DocumentLocation>();
        }
    }

    public bool IsFull()
    {
        return KeyCount >= _maxKeys;
    }

    public byte[] Serialize()
    {
        byte[] buffer = new byte[_pageSize];
        int offset = 0;

        buffer[offset] = PageType;
        offset += 1;

        buffer[offset] = (byte)NodeType;
        offset += 1;

        BinaryHelper.WriteUInt16LE(buffer, offset, KeyCount);
        offset += 2;

        BinaryHelper.WriteInt32LE(buffer, offset, NextLeaf);
        offset += 4;

        BinaryHelper.WriteUInt16LE(buffer, offset, (ushort)_maxKeys);
        offset += 2;

        for (int i = 0; i < KeyCount; i++)
        {
            byte[] key = Keys[i];
            BinaryHelper.WriteUInt16LE(buffer, offset, (ushort)key.Length);
            offset += 2;
            Array.Copy(key, 0, buffer, offset, key.Length);
            offset += key.Length;
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

        return buffer;
    }

    public static SecondaryIndexNode Deserialize(byte[] buffer, int pageSize)
    {
        int offset = 0;

        byte pageType = buffer[offset];
        offset += 1;

        BTreeNodeType nodeType = (BTreeNodeType)buffer[offset];
        offset += 1;

        ushort keyCount = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        int nextLeaf = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        ushort maxKeys = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, nodeType);
        node.PageType = pageType;
        node.KeyCount = keyCount;
        node.NextLeaf = nextLeaf;

        for (int i = 0; i < keyCount; i++)
        {
            ushort keyLength = BinaryHelper.ReadUInt16LE(buffer, offset);
            offset += 2;
            byte[] key = new byte[keyLength];
            Array.Copy(buffer, offset, key, 0, keyLength);
            node.Keys.Add(key);
            offset += keyLength;
        }

        if (nodeType == BTreeNodeType.Internal)
        {
            for (int i = 0; i <= keyCount; i++)
            {
                int childPageId = BinaryHelper.ReadInt32LE(buffer, offset);
                node.ChildPageIds.Add(childPageId);
                offset += 4;
            }
        }
        else
        {
            for (int i = 0; i < keyCount; i++)
            {
                int pageId = BinaryHelper.ReadInt32LE(buffer, offset);
                offset += 4;
                int slotIndex = BinaryHelper.ReadInt32LE(buffer, offset);
                offset += 4;
                node.LeafValues.Add(new DocumentLocation(pageId, slotIndex));
            }
        }

        return node;
    }

    public static int CompareKeys(byte[] a, byte[] b)
    {
        int minLength = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLength; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
    }
}
