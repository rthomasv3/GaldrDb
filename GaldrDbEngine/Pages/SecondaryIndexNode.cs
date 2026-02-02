using System;
using System.Collections.Generic;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbEngine.Pages;

internal class SecondaryIndexNode
{
    // Buffer layout constants
    private const int OFFSET_PAGE_TYPE = 0;
    private const int OFFSET_NODE_TYPE = 1;
    private const int OFFSET_KEY_COUNT = 2;
    private const int OFFSET_NEXT_LEAF = 4;
    private const int OFFSET_MAX_KEYS = 8;
    private const int OFFSET_KEYS = 10;

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
        bool full = KeyCount >= _maxKeys;
        if (!full)
        {
            full = GetCurrentSerializedSize() >= _pageSize - 128;
        }
        return full;
    }

    public int GetCurrentSerializedSize()
    {
        int size = 10;

        for (int i = 0; i < KeyCount; i++)
        {
            size += 2 + Keys[i].Length;
        }

        if (NodeType == BTreeNodeType.Internal)
        {
            size += (KeyCount + 1) * 4;
        }
        else
        {
            size += KeyCount * 8;
        }

        return size;
    }

    public bool CanMergeWith(SecondaryIndexNode other, int separatorKeyLength)
    {
        int baseOverhead = 10;
        int combinedKeySize = 0;

        for (int i = 0; i < KeyCount; i++)
        {
            combinedKeySize += 2 + Keys[i].Length;
        }

        for (int i = 0; i < other.KeyCount; i++)
        {
            combinedKeySize += 2 + other.Keys[i].Length;
        }

        if (NodeType == BTreeNodeType.Internal)
        {
            combinedKeySize += 2 + separatorKeyLength;
        }

        int combinedValueSize;
        if (NodeType == BTreeNodeType.Internal)
        {
            combinedValueSize = (KeyCount + other.KeyCount + 2) * 4;
        }
        else
        {
            combinedValueSize = (KeyCount + other.KeyCount) * 8;
        }

        int totalSize = baseOverhead + combinedKeySize + combinedValueSize;
        return totalSize <= _pageSize - 64;
    }

    public int GetMinKeys()
    {
        return _maxKeys / 2;
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

    // ===== Static buffer access methods for direct buffer manipulation =====

    public static bool IsLeafNode(byte[] buffer)
    {
        return (BTreeNodeType)buffer[OFFSET_NODE_TYPE] == BTreeNodeType.Leaf;
    }

    public static ushort GetKeyCount(byte[] buffer)
    {
        return BinaryHelper.ReadUInt16LE(buffer, OFFSET_KEY_COUNT);
    }

    public static void SetKeyCount(byte[] buffer, ushort count)
    {
        BinaryHelper.WriteUInt16LE(buffer, OFFSET_KEY_COUNT, count);
    }

    public static ushort GetMaxKeys(byte[] buffer)
    {
        return BinaryHelper.ReadUInt16LE(buffer, OFFSET_MAX_KEYS);
    }

    /// <summary>
    /// Calculates the offset where keys end (where values begin).
    /// Optionally populates keyOffsets and keyLengths arrays.
    /// </summary>
    public static int GetKeysEndOffset(byte[] buffer, int keyCount, int[] keyOffsets, int[] keyLengths)
    {
        int offset = OFFSET_KEYS;
        for (int i = 0; i < keyCount; i++)
        {
            ushort keyLength = BinaryHelper.ReadUInt16LE(buffer, offset);
            offset += 2;
            if (keyOffsets != null)
            {
                keyOffsets[i] = offset;
            }
            if (keyLengths != null)
            {
                keyLengths[i] = keyLength;
            }
            offset += keyLength;
        }
        return offset;
    }

    /// <summary>
    /// Gets the total serialized size of the node in the buffer.
    /// </summary>
    public static int GetSerializedSize(byte[] buffer, int keyCount, bool isLeaf)
    {
        int keysEndOffset = GetKeysEndOffset(buffer, keyCount, null, null);
        int size;
        if (isLeaf)
        {
            size = keysEndOffset + (keyCount * 8);
        }
        else
        {
            size = keysEndOffset + ((keyCount + 1) * 4);
        }
        return size;
    }

    /// <summary>
    /// Checks if the node is full based on key count and serialized size.
    /// </summary>
    public static bool IsNodeFull(byte[] buffer, int maxKeys, int pageSize)
    {
        ushort keyCount = GetKeyCount(buffer);
        bool full = keyCount >= maxKeys;
        if (!full)
        {
            bool isLeaf = IsLeafNode(buffer);
            int currentSize = GetSerializedSize(buffer, keyCount, isLeaf);
            full = currentSize >= pageSize - 128;
        }
        return full;
    }

    /// <summary>
    /// Check if node is safe for insert (won't split).
    /// A node is safe if it has room for at least one more key.
    /// </summary>
    public static bool IsSafeForInsert(byte[] buffer, int maxKeys, int pageSize)
    {
        return !IsNodeFull(buffer, maxKeys, pageSize);
    }

    /// <summary>
    /// Check if node is safe for delete (won't underflow/rebalance).
    /// A node is delete-safe if it has more than the minimum required keys.
    /// </summary>
    public static bool IsSafeForDelete(byte[] buffer, int maxKeys)
    {
        int minKeys = maxKeys / 2;
        return GetKeyCount(buffer) > minKeys;
    }

    /// <summary>
    /// Finds the position where a key should be inserted (for leaf) or the child index to descend (for internal).
    /// Returns the index where key should go (0 to keyCount).
    /// </summary>
    public static int FindKeyPosition(byte[] buffer, int keyCount, ReadOnlySpan<byte> targetKey)
    {
        int offset = OFFSET_KEYS;
        int position = keyCount;

        for (int i = 0; i < keyCount; i++)
        {
            ushort keyLength = BinaryHelper.ReadUInt16LE(buffer, offset);
            offset += 2;

            ReadOnlySpan<byte> nodeKey = buffer.AsSpan(offset, keyLength);
            int cmp = targetKey.SequenceCompareTo(nodeKey);

            if (cmp <= 0)
            {
                position = i;
                break;
            }

            offset += keyLength;
        }

        return position;
    }

    /// <summary>
    /// Gets the child page ID at the given index for an internal node.
    /// </summary>
    public static int GetChildPageId(byte[] buffer, int keyCount, int childIndex)
    {
        int valuesOffset = GetKeysEndOffset(buffer, keyCount, null, null);
        return BinaryHelper.ReadInt32LE(buffer, valuesOffset + (childIndex * 4));
    }

    /// <summary>
    /// Inserts a key-value pair into a leaf node buffer.
    /// Returns the new key count.
    /// </summary>
    public static int InsertIntoLeaf(byte[] buffer, int keyCount, byte[] key, DocumentLocation value, int pageSize)
    {
        // Find insert position
        ReadOnlySpan<byte> keySpan = key.AsSpan();
        int insertPos = FindKeyPosition(buffer, keyCount, keySpan);

        // Calculate where keys currently end (this is where values start)
        int keysEndOffset = GetKeysEndOffset(buffer, keyCount, null, null);
        int oldValuesStart = keysEndOffset;

        // Calculate offset where we need to insert the new key
        int insertKeyOffset = OFFSET_KEYS;
        for (int i = 0; i < insertPos; i++)
        {
            ushort len = BinaryHelper.ReadUInt16LE(buffer, insertKeyOffset);
            insertKeyOffset += 2 + len;
        }

        int keyEntrySize = 2 + key.Length;
        int keysToShift = keyCount - insertPos;

        // Calculate new values region start (after keys grow by keyEntrySize)
        int newValuesStart = keysEndOffset + keyEntrySize;

        // STEP 1: Move ALL existing values to their new location first (shifted right by keyEntrySize)
        // We must do this BEFORE shifting keys, otherwise keys would overwrite values
        // Use memmove semantics (copy backwards to handle overlap)
        if (keyCount > 0)
        {
            for (int i = keyCount - 1; i >= 0; i--)
            {
                int oldOffset = oldValuesStart + (i * 8);
                int newOffset = newValuesStart + (i * 8);
                // Shift by one more slot for values after insertPos to make room for new value
                if (i >= insertPos)
                {
                    newOffset += 8;
                }
                // Copy 8 bytes (PageId + SlotIndex)
                buffer[newOffset] = buffer[oldOffset];
                buffer[newOffset + 1] = buffer[oldOffset + 1];
                buffer[newOffset + 2] = buffer[oldOffset + 2];
                buffer[newOffset + 3] = buffer[oldOffset + 3];
                buffer[newOffset + 4] = buffer[oldOffset + 4];
                buffer[newOffset + 5] = buffer[oldOffset + 5];
                buffer[newOffset + 6] = buffer[oldOffset + 6];
                buffer[newOffset + 7] = buffer[oldOffset + 7];
            }
        }

        // STEP 2: Shift existing keys after insertPos to make room for new key
        if (keysToShift > 0)
        {
            int shiftStartOffset = insertKeyOffset;
            int bytesToShift = keysEndOffset - shiftStartOffset;
            // Copy backwards to handle overlap correctly
            for (int i = bytesToShift - 1; i >= 0; i--)
            {
                buffer[shiftStartOffset + keyEntrySize + i] = buffer[shiftStartOffset + i];
            }
        }

        // STEP 3: Write the new key
        BinaryHelper.WriteUInt16LE(buffer, insertKeyOffset, (ushort)key.Length);
        Array.Copy(key, 0, buffer, insertKeyOffset + 2, key.Length);

        // STEP 4: Write the new value
        int newValueOffset = newValuesStart + (insertPos * 8);
        BinaryHelper.WriteInt32LE(buffer, newValueOffset, value.PageId);
        BinaryHelper.WriteInt32LE(buffer, newValueOffset + 4, value.SlotIndex);

        // Update key count
        int newKeyCount = keyCount + 1;
        SetKeyCount(buffer, (ushort)newKeyCount);

        return newKeyCount;
    }
}
