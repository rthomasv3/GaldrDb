using System;
using System.Collections.Generic;

namespace GaldrDbEngine.Utilities;

internal static class AllocTracer
{
    public static bool Enabled { get; set; } = false;

    private static long _lastCheckpoint;
    private static List<(string Phase, long Bytes)> _phases = new();

    public static void Reset()
    {
        _lastCheckpoint = GC.GetAllocatedBytesForCurrentThread();
        _phases.Clear();
    }

    public static void Checkpoint(string phase)
    {
        if (!Enabled) return;

        long current = GC.GetAllocatedBytesForCurrentThread();
        long delta = current - _lastCheckpoint;
        _phases.Add((phase, delta));
        _lastCheckpoint = current;
    }

    public static void PrintSummary()
    {
        if (!Enabled) return;

        Console.WriteLine("=== Allocation Trace ===");
        long total = 0;
        foreach ((string phase, long bytes) in _phases)
        {
            Console.WriteLine($"  {phase}: {bytes} bytes");
            total += bytes;
        }
        Console.WriteLine($"  TOTAL: {total} bytes");
        Console.WriteLine();
    }

    public static List<(string Phase, long Bytes)> GetPhases()
    {
        return new List<(string, long)>(_phases);
    }
}
