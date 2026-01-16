using System.Collections.Concurrent;

namespace GaldrDbEngine.Utilities;

internal static class JsonWriterPool
{
    public const int DEFAULT_BUFFER_CAPACITY = 4096;

    private static readonly ConcurrentBag<PooledJsonWriter> _pool = new();
    private static int _bufferCapacity = DEFAULT_BUFFER_CAPACITY;
    private static int _hits;
    private static int _misses;

    public static (int Hits, int Misses) Stats => (_hits, _misses);

    public static void ResetStats()
    {
        _hits = 0;
        _misses = 0;
    }

    public static void Configure(int bufferCapacity)
    {
        _bufferCapacity = bufferCapacity;
    }

    public static PooledJsonWriter Rent()
    {
        PooledJsonWriter result;

        if (_pool.TryTake(out PooledJsonWriter writer))
        {
            _hits++;
            writer.Reset();
            result = writer;
        }
        else
        {
            _misses++;
            result = new PooledJsonWriter(_bufferCapacity);
        }

        return result;
    }

    public static void Return(PooledJsonWriter writer)
    {
        if (writer != null)
        {
            writer.PrepareForReturn();
            _pool.Add(writer);
        }
    }

    public static void Warmup(int count)
    {
        for (int i = 0; i < count; i++)
        {
            _pool.Add(new PooledJsonWriter(_bufferCapacity));
        }
    }
}
