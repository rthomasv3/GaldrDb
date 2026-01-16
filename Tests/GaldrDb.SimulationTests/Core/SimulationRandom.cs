using System;

namespace GaldrDb.SimulationTests.Core;

public class SimulationRandom
{
    private readonly Random _rng;
    private readonly int _seed;

    public SimulationRandom(int seed)
    {
        _seed = seed;
        _rng = new Random(seed);
    }

    public int Seed => _seed;

    public int Next()
    {
        return _rng.Next();
    }

    public int Next(int maxValue)
    {
        return _rng.Next(maxValue);
    }

    public int Next(int minValue, int maxValue)
    {
        return _rng.Next(minValue, maxValue);
    }

    public double NextDouble()
    {
        return _rng.NextDouble();
    }

    public uint NextUInt()
    {
        return (uint)_rng.Next();
    }

    public void NextBytes(byte[] buffer)
    {
        _rng.NextBytes(buffer);
    }

    public void NextBytes(Span<byte> buffer)
    {
        _rng.NextBytes(buffer);
    }
}
