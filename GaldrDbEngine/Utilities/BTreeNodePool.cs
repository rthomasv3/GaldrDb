using System.Collections.Concurrent;
using GaldrDbEngine.Pages;

namespace GaldrDbEngine.Utilities;

public static class BTreeNodePool
{
    private static readonly ConcurrentDictionary<(int PageSize, int Order), ConcurrentBag<BTreeNode>> _pools = new();

    public static BTreeNode Rent(int pageSize, int order, BTreeNodeType nodeType)
    {
        BTreeNode result;
        ConcurrentBag<BTreeNode> pool = _pools.GetOrAdd((pageSize, order), _ => new ConcurrentBag<BTreeNode>());

        if (pool.TryTake(out BTreeNode node))
        {
            node.Reset(nodeType);
            node.EnsureListsForNodeType(nodeType);
            result = node;
        }
        else
        {
            result = new BTreeNode(pageSize, order, nodeType);
        }

        return result;
    }

    public static void Return(BTreeNode node)
    {
        if (node != null)
        {
            node.ReturnLists();
            ConcurrentBag<BTreeNode> pool = _pools.GetOrAdd((node.PageSize, node.Order), _ => new ConcurrentBag<BTreeNode>());
            pool.Add(node);
        }
    }
}
