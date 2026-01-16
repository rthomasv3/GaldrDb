using System.Collections.Concurrent;
using GaldrDbEngine.Pages;

namespace GaldrDbEngine.Utilities;

internal static class SecondaryIndexNodePool
{
    private static readonly ConcurrentDictionary<(int PageSize, int MaxKeys), ConcurrentBag<SecondaryIndexNode>> _pools = new();

    public static SecondaryIndexNode Rent(int pageSize, int maxKeys, BTreeNodeType nodeType)
    {
        SecondaryIndexNode result;
        ConcurrentBag<SecondaryIndexNode> pool = _pools.GetOrAdd((pageSize, maxKeys), _ => new ConcurrentBag<SecondaryIndexNode>());

        if (pool.TryTake(out SecondaryIndexNode node))
        {
            node.Reset(nodeType);
            node.EnsureListsForNodeType(nodeType);
            result = node;
        }
        else
        {
            result = new SecondaryIndexNode(pageSize, maxKeys, nodeType);
        }

        return result;
    }

    public static void Return(SecondaryIndexNode node)
    {
        if (node != null)
        {
            node.ReturnLists();
            ConcurrentBag<SecondaryIndexNode> pool = _pools.GetOrAdd((node.PageSize, node.MaxKeys), _ => new ConcurrentBag<SecondaryIndexNode>());
            pool.Add(node);
        }
    }

    public static void Warmup(int pageSize, int maxKeys, int count)
    {
        ConcurrentBag<SecondaryIndexNode> pool = _pools.GetOrAdd((pageSize, maxKeys), _ => new ConcurrentBag<SecondaryIndexNode>());

        for (int i = 0; i < count; i++)
        {
            SecondaryIndexNode node = new SecondaryIndexNode(pageSize, maxKeys, BTreeNodeType.Leaf);
            node.ReturnLists();
            pool.Add(node);
        }
    }
}
