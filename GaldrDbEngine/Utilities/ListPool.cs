using System.Collections.Concurrent;
using System.Collections.Generic;

namespace GaldrDbEngine.Utilities;

internal static class ListPool<T>
{
    private static readonly ConcurrentDictionary<int, ConcurrentBag<List<T>>> _pools = new();
    private static int _hits;
    private static int _misses;
    private static int _capacityMismatches;

    public static (int Hits, int Misses, int CapacityMismatches) Stats => (_hits, _misses, _capacityMismatches);

    public static void ResetStats()
    {
        _hits = 0;
        _misses = 0;
        _capacityMismatches = 0;
    }

    public static List<T> Rent(int capacity)
    {
        List<T> result;
        ConcurrentBag<List<T>> pool = _pools.GetOrAdd(capacity, _ => new ConcurrentBag<List<T>>());

        if (pool.TryTake(out List<T> list))
        {
            _hits++;
            result = list;
        }
        else
        {
            _misses++;
            result = new List<T>(capacity);
        }

        return result;
    }

    public static void Return(List<T> list)
    {
        if (list != null)
        {
            int originalCapacity = list.Capacity;
            list.Clear();
            if (list.Capacity != originalCapacity)
            {
                _capacityMismatches++;
            }
            ConcurrentBag<List<T>> pool = _pools.GetOrAdd(list.Capacity, _ => new ConcurrentBag<List<T>>());
            pool.Add(list);
        }
    }
}
