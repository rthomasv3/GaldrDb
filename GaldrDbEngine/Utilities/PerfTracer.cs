using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace GaldrDbEngine.Utilities;

internal static class PerfTracer
{
    public static bool Enabled { get; set; } = false;

    private static long _startTicks;
    private static long _lastCheckpoint;
    private static List<(string Phase, double Microseconds)> _phases = new();

    public static void Reset()
    {
        _startTicks = Stopwatch.GetTimestamp();
        _lastCheckpoint = _startTicks;
        _phases.Clear();
    }

    public static void Checkpoint(string phase)
    {
        if (!Enabled) return;

        long current = Stopwatch.GetTimestamp();
        double deltaMicroseconds = (current - _lastCheckpoint) * 1_000_000.0 / Stopwatch.Frequency;
        _phases.Add((phase, deltaMicroseconds));
        _lastCheckpoint = current;
    }

    public static void PrintSummary()
    {
        if (!Enabled) return;

        Console.WriteLine("=== Performance Trace ===");
        double total = 0;
        foreach ((string phase, double us) in _phases)
        {
            Console.WriteLine($"  {phase}: {us:F2} µs");
            total += us;
        }
        Console.WriteLine($"  TOTAL: {total:F2} µs");
        Console.WriteLine();
    }

    public static void PrintAggregated()
    {
        if (!Enabled) return;

        Dictionary<string, (double Total, int Count)> aggregated = new();

        foreach ((string phase, double us) in _phases)
        {
            if (aggregated.TryGetValue(phase, out (double Total, int Count) existing))
            {
                aggregated[phase] = (existing.Total + us, existing.Count + 1);
            }
            else
            {
                aggregated[phase] = (us, 1);
            }
        }

        Console.WriteLine("=== Aggregated Performance Trace ===");
        double total = 0;
        foreach (KeyValuePair<string, (double Total, int Count)> kvp in aggregated)
        {
            double avg = kvp.Value.Total / kvp.Value.Count;
            Console.WriteLine($"  {kvp.Key}: {kvp.Value.Total:F2} µs total, {avg:F2} µs avg ({kvp.Value.Count} calls)");
            total += kvp.Value.Total;
        }
        Console.WriteLine($"  TOTAL: {total:F2} µs");
        Console.WriteLine();
    }

    public static List<(string Phase, double Microseconds)> GetPhases()
    {
        return new List<(string, double)>(_phases);
    }
}
