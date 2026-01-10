using System.Collections.Concurrent;
using System.Collections.Generic;

namespace GaldrDbEngine.Utilities;

public static class ListPool<T>
{
    private static readonly ConcurrentDictionary<int, ConcurrentBag<List<T>>> _pools = new();

    public static List<T> Rent(int capacity)
    {
        List<T> result;
        ConcurrentBag<List<T>> pool = _pools.GetOrAdd(capacity, _ => new ConcurrentBag<List<T>>());

        if (pool.TryTake(out List<T> list))
        {
            result = list;
        }
        else
        {
            result = new List<T>(capacity);
        }

        return result;
    }

    public static void Return(List<T> list)
    {
        if (list != null)
        {
            list.Clear();
            ConcurrentBag<List<T>> pool = _pools.GetOrAdd(list.Capacity, _ => new ConcurrentBag<List<T>>());
            pool.Add(list);
        }
    }
}
