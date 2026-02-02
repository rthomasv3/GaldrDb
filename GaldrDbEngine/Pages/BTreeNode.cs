using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

internal class BTreeNode
{
    private const int HEADER_SIZE = 8;

    // Buffer layout offsets
    private const int OFFSET_PAGE_TYPE = 0;
    private const int OFFSET_NODE_TYPE = 1;
    private const int OFFSET_KEY_COUNT = 2;
    private const int OFFSET_NEXT_LEAF = 4;
    private const int OFFSET_KEYS = 8;

    #region Static Buffer Access Methods

    public static bool IsLeafNode(byte[] buffer)
    {
        return buffer[OFFSET_NODE_TYPE] == (byte)BTreeNodeType.Leaf;
    }

    public static ushort GetKeyCount(byte[] buffer)
    {
        return BinaryHelper.ReadUInt16LE(buffer, OFFSET_KEY_COUNT);
    }

    public static void SetKeyCount(byte[] buffer, ushort count)
    {
        BinaryHelper.WriteUInt16LE(buffer, OFFSET_KEY_COUNT, count);
    }

    public static int GetKey(byte[] buffer, int index)
    {
        return BinaryHelper.ReadInt32LE(buffer, OFFSET_KEYS + index * 4);
    }

    public static void SetKey(byte[] buffer, int index, int key)
    {
        BinaryHelper.WriteInt32LE(buffer, OFFSET_KEYS + index * 4, key);
    }

    public static int GetChildPageId(byte[] buffer, int keyCount, int childIndex)
    {
        int childrenOffset = OFFSET_KEYS + keyCount * 4;
        return BinaryHelper.ReadInt32LE(buffer, childrenOffset + childIndex * 4);
    }

    public static void SetChildPageId(byte[] buffer, int keyCount, int childIndex, int pageId)
    {
        int childrenOffset = OFFSET_KEYS + keyCount * 4;
        BinaryHelper.WriteInt32LE(buffer, childrenOffset + childIndex * 4, pageId);
    }

    public static DocumentLocation GetLeafValue(byte[] buffer, int keyCount, int index)
    {
        int valuesOffset = OFFSET_KEYS + keyCount * 4 + index * 8;
        int pageId = BinaryHelper.ReadInt32LE(buffer, valuesOffset);
        int slotIndex = BinaryHelper.ReadInt32LE(buffer, valuesOffset + 4);
        return new DocumentLocation(pageId, slotIndex);
    }

    public static void SetLeafValue(byte[] buffer, int keyCount, int index, DocumentLocation location)
    {
        int valuesOffset = OFFSET_KEYS + keyCount * 4 + index * 8;
        BinaryHelper.WriteInt32LE(buffer, valuesOffset, location.PageId);
        BinaryHelper.WriteInt32LE(buffer, valuesOffset + 4, location.SlotIndex);
    }

    /// <summary>
    /// Binary search for the position where key should be inserted.
    /// Returns the index of the first key greater than or equal to the target,
    /// or keyCount if all keys are less than target.
    /// </summary>
    public static int FindKeyPosition(byte[] buffer, int keyCount, int targetKey)
    {
        int left = 0;
        int right = keyCount;

        while (left < right)
        {
            int mid = left + (right - left) / 2;
            int midKey = GetKey(buffer, mid);

            if (midKey < targetKey)
            {
                left = mid + 1;
            }
            else
            {
                right = mid;
            }
        }

        return left;
    }

    /// <summary>
    /// Find the child index to follow for a given key in an internal node.
    /// </summary>
    public static int FindChildIndex(byte[] buffer, int keyCount, int targetKey)
    {
        return FindKeyPosition(buffer, keyCount, targetKey);
    }

    /// <summary>
    /// Check if node is full (for a given order).
    /// </summary>
    public static bool IsNodeFull(byte[] buffer, int order)
    {
        return GetKeyCount(buffer) >= order - 1;
    }

    /// <summary>
    /// Check if node is safe for insert (won't split).
    /// A node is insert-safe if it has room for at least one more key.
    /// </summary>
    public static bool IsSafeForInsert(byte[] buffer, int order)
    {
        return GetKeyCount(buffer) < order - 1;
    }

    /// <summary>
    /// Check if node is safe for delete (won't underflow/rebalance).
    /// A node is delete-safe if it has more than the minimum required keys.
    /// </summary>
    public static bool IsSafeForDelete(byte[] buffer, int order)
    {
        int minKeys = (order - 1) / 2;
        return GetKeyCount(buffer) > minKeys;
    }

    /// <summary>
    /// Insert a key and value into a leaf node at the correct position.
    /// Shifts existing entries to make room. Returns the insert position.
    /// </summary>
    public static int InsertIntoLeaf(byte[] buffer, int keyCount, int key, DocumentLocation value, int order)
    {
        int insertPos = FindKeyPosition(buffer, keyCount, key);

        int oldValuesStart = OFFSET_KEYS + keyCount * 4;
        int newValuesStart = OFFSET_KEYS + (keyCount + 1) * 4;

        // FIRST: Shift ALL values to their new positions (working backwards to avoid overwrites)
        // Values at index >= insertPos also shift one position right in the array
        for (int i = keyCount - 1; i >= 0; i--)
        {
            int oldValueOffset = oldValuesStart + i * 8;
            int newIndex = (i >= insertPos) ? i + 1 : i;
            int newValueOffset = newValuesStart + newIndex * 8;

            int pageId = BinaryHelper.ReadInt32LE(buffer, oldValueOffset);
            int slotIndex = BinaryHelper.ReadInt32LE(buffer, oldValueOffset + 4);
            BinaryHelper.WriteInt32LE(buffer, newValueOffset, pageId);
            BinaryHelper.WriteInt32LE(buffer, newValueOffset + 4, slotIndex);
        }

        // SECOND: Shift keys at and after insertPos (safe now that values are moved)
        for (int i = keyCount - 1; i >= insertPos; i--)
        {
            int existingKey = GetKey(buffer, i);
            SetKey(buffer, i + 1, existingKey);
        }

        // Insert new key and value
        SetKey(buffer, insertPos, key);
        int valueOffset = newValuesStart + insertPos * 8;
        BinaryHelper.WriteInt32LE(buffer, valueOffset, value.PageId);
        BinaryHelper.WriteInt32LE(buffer, valueOffset + 4, value.SlotIndex);

        // Update key count
        SetKeyCount(buffer, (ushort)(keyCount + 1));

        return insertPos;
    }

    #endregion

    private readonly int _pageSize;
    private readonly int _order;

    public int PageSize => _pageSize;
    public int Order => _order;

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
        Keys = ListPool<int>.Rent(order - 1);

        if (nodeType == BTreeNodeType.Internal)
        {
            ChildPageIds = ListPool<int>.Rent(order);
            LeafValues = null;
        }
        else
        {
            ChildPageIds = null;
            LeafValues = ListPool<DocumentLocation>.Rent(order - 1);
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

    public int GetMinKeys()
    {
        return (_order - 1) / 2;
    }

    public bool IsUnderflow()
    {
        return KeyCount < GetMinKeys();
    }

    public bool CanLendKey()
    {
        return KeyCount > GetMinKeys();
    }

    public void Reset(BTreeNodeType nodeType)
    {
        PageType = PageConstants.PAGE_TYPE_BTREE;
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
            Keys = ListPool<int>.Rent(_order - 1);
        }

        if (nodeType == BTreeNodeType.Internal)
        {
            if (ChildPageIds == null)
            {
                ChildPageIds = ListPool<int>.Rent(_order);
            }
        }
        else
        {
            if (LeafValues == null)
            {
                LeafValues = ListPool<DocumentLocation>.Rent(_order - 1);
            }
        }
    }

    public void ReturnLists()
    {
        ListPool<int>.Return(Keys);
        Keys = null;
        ListPool<int>.Return(ChildPageIds);
        ChildPageIds = null;
        ListPool<DocumentLocation>.Return(LeafValues);
        LeafValues = null;
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

        node.NodeType = nodeType;
        node.EnsureListsForNodeType(nodeType);

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
