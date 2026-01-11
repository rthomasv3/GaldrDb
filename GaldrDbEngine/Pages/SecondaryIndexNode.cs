using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

public class SecondaryIndexNode
{
    private readonly int _pageSize;
    private readonly int _maxKeys;
    private byte[] _keyDataBuffer;
    private int _keyDataBufferSize;

    public int PageSize => _pageSize;
    public int MaxKeys => _maxKeys;

    public byte PageType { get; set; }
    public BTreeNodeType NodeType { get; set; }
    public ushort KeyCount { get; set; }
    public int NextLeaf { get; set; }
    public List<KeyBuffer> Keys { get; set; }
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
        Keys = ListPool<KeyBuffer>.Rent(maxKeys);

        if (nodeType == BTreeNodeType.Internal)
        {
            ChildPageIds = ListPool<int>.Rent(maxKeys + 1);
            LeafValues = null;
        }
        else
        {
            ChildPageIds = null;
            LeafValues = ListPool<DocumentLocation>.Rent(maxKeys);
        }
    }

    public bool IsFull()
    {
        return KeyCount >= _maxKeys;
    }

    public void Reset(BTreeNodeType nodeType)
    {
        PageType = PageConstants.PAGE_TYPE_SECONDARY_INDEX;
        NodeType = nodeType;
        KeyCount = 0;
        NextLeaf = 0;

        Keys?.Clear();
        ChildPageIds?.Clear();
        LeafValues?.Clear();
    }

    public void EnsureListsForNodeType(BTreeNodeType nodeType)
    {
        if (Keys == null)
        {
            Keys = ListPool<KeyBuffer>.Rent(_maxKeys);
        }

        if (nodeType == BTreeNodeType.Internal)
        {
            if (ChildPageIds == null)
            {
                ChildPageIds = ListPool<int>.Rent(_maxKeys + 1);
            }
        }
        else
        {
            if (LeafValues == null)
            {
                LeafValues = ListPool<DocumentLocation>.Rent(_maxKeys);
            }
        }
    }

    public void ReturnLists()
    {
        ListPool<KeyBuffer>.Return(Keys);
        Keys = null;
        ListPool<int>.Return(ChildPageIds);
        ChildPageIds = null;
        ListPool<DocumentLocation>.Return(LeafValues);
        LeafValues = null;
    }

    public void SerializeTo(byte[] buffer)
    {
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

        BinaryHelper.WriteUInt16LE(buffer, offset, (ushort)_maxKeys);
        offset += 2;

        for (int i = 0; i < KeyCount; i++)
        {
            KeyBuffer key = Keys[i];
            ReadOnlySpan<byte> keySpan = key.AsSpan();
            BinaryHelper.WriteUInt16LE(buffer, offset, (ushort)key.Length);
            offset += 2;
            keySpan.CopyTo(buffer.AsSpan(offset, key.Length));
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
    }

    public void EnsureKeyDataBuffer(int requiredSize)
    {
        if (_keyDataBuffer == null || _keyDataBufferSize < requiredSize)
        {
            if (_keyDataBuffer != null)
            {
                BufferPool.Return(_keyDataBuffer);
            }
            _keyDataBuffer = BufferPool.Rent(requiredSize);
            _keyDataBufferSize = requiredSize;
        }
    }

    public void ReturnKeyDataBuffer()
    {
        if (_keyDataBuffer != null)
        {
            BufferPool.Return(_keyDataBuffer);
            _keyDataBuffer = null;
            _keyDataBufferSize = 0;
        }
    }

    public static void DeserializeTo(byte[] buffer, SecondaryIndexNode node)
    {
        int offset = 0;

        node.PageType = buffer[offset];
        offset += 1;

        BTreeNodeType nodeType = (BTreeNodeType)buffer[offset];
        offset += 1;

        ushort keyCount = BinaryHelper.ReadUInt16LE(buffer, offset);
        offset += 2;

        node.NextLeaf = BinaryHelper.ReadInt32LE(buffer, offset);
        offset += 4;

        offset += 2;

        node.NodeType = nodeType;
        node.KeyCount = keyCount;
        node.EnsureListsForNodeType(nodeType);

        node.Keys?.Clear();
        node.ChildPageIds?.Clear();
        node.LeafValues?.Clear();

        int totalKeyBytes = 0;
        int scanOffset = offset;
        for (int i = 0; i < keyCount; i++)
        {
            ushort keyLength = BinaryHelper.ReadUInt16LE(buffer, scanOffset);
            scanOffset += 2;
            totalKeyBytes += keyLength;
            scanOffset += keyLength;
        }

        if (totalKeyBytes > 0)
        {
            node.EnsureKeyDataBuffer(totalKeyBytes);
        }

        int keyDataOffset = 0;
        for (int i = 0; i < keyCount; i++)
        {
            ushort keyLength = BinaryHelper.ReadUInt16LE(buffer, offset);
            offset += 2;
            Array.Copy(buffer, offset, node._keyDataBuffer, keyDataOffset, keyLength);
            node.Keys.Add(new KeyBuffer(node._keyDataBuffer, keyDataOffset, keyLength));
            keyDataOffset += keyLength;
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
    }
}
