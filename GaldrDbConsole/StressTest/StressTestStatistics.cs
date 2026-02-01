using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace GaldrDbConsole.StressTest;

public class StressTestStatistics
{
    private long _insertsCompleted;
    private long _readsCompleted;
    private long _updatesCompleted;
    private long _deletesCompleted;

    private long _insertsFailed;
    private long _readsFailed;
    private long _updatesFailed;
    private long _deletesFailed;

    private long _writeConflicts;
    private long _retriesPerformed;
    private long _maxRetriesExceeded;

    private long _validationErrors;
    private long _unexpectedErrors;

    private readonly List<double> _insertLatenciesMs;
    private readonly List<double> _readLatenciesMs;
    private readonly List<double> _updateLatenciesMs;
    private readonly List<double> _deleteLatenciesMs;
    private readonly object _latencyLock;

    public StressTestStatistics()
    {
        _insertLatenciesMs = new List<double>();
        _readLatenciesMs = new List<double>();
        _updateLatenciesMs = new List<double>();
        _deleteLatenciesMs = new List<double>();
        _latencyLock = new object();
    }

    public long TotalOperations => Interlocked.Read(ref _insertsCompleted) +
                                    Interlocked.Read(ref _readsCompleted) +
                                    Interlocked.Read(ref _updatesCompleted) +
                                    Interlocked.Read(ref _deletesCompleted) +
                                    Interlocked.Read(ref _insertsFailed) +
                                    Interlocked.Read(ref _readsFailed) +
                                    Interlocked.Read(ref _updatesFailed) +
                                    Interlocked.Read(ref _deletesFailed);

    public long TotalSuccessful => Interlocked.Read(ref _insertsCompleted) +
                                    Interlocked.Read(ref _readsCompleted) +
                                    Interlocked.Read(ref _updatesCompleted) +
                                    Interlocked.Read(ref _deletesCompleted);

    public long TotalFailed => Interlocked.Read(ref _insertsFailed) +
                                Interlocked.Read(ref _readsFailed) +
                                Interlocked.Read(ref _updatesFailed) +
                                Interlocked.Read(ref _deletesFailed);

    public long InsertsCompleted => Interlocked.Read(ref _insertsCompleted);
    public long ReadsCompleted => Interlocked.Read(ref _readsCompleted);
    public long UpdatesCompleted => Interlocked.Read(ref _updatesCompleted);
    public long DeletesCompleted => Interlocked.Read(ref _deletesCompleted);

    public long InsertsFailed => Interlocked.Read(ref _insertsFailed);
    public long ReadsFailed => Interlocked.Read(ref _readsFailed);
    public long UpdatesFailed => Interlocked.Read(ref _updatesFailed);
    public long DeletesFailed => Interlocked.Read(ref _deletesFailed);

    public long WriteConflicts => Interlocked.Read(ref _writeConflicts);
    public long RetriesPerformed => Interlocked.Read(ref _retriesPerformed);
    public long MaxRetriesExceeded => Interlocked.Read(ref _maxRetriesExceeded);
    public long ValidationErrors => Interlocked.Read(ref _validationErrors);
    public long UnexpectedErrors => Interlocked.Read(ref _unexpectedErrors);

    public void RecordInsert(bool success, double latencyMs)
    {
        if (success)
        {
            Interlocked.Increment(ref _insertsCompleted);
        }
        else
        {
            Interlocked.Increment(ref _insertsFailed);
        }

        lock (_latencyLock)
        {
            _insertLatenciesMs.Add(latencyMs);
        }
    }

    public void RecordRead(bool success, double latencyMs)
    {
        if (success)
        {
            Interlocked.Increment(ref _readsCompleted);
        }
        else
        {
            Interlocked.Increment(ref _readsFailed);
        }

        lock (_latencyLock)
        {
            _readLatenciesMs.Add(latencyMs);
        }
    }

    public void RecordUpdate(bool success, double latencyMs)
    {
        if (success)
        {
            Interlocked.Increment(ref _updatesCompleted);
        }
        else
        {
            Interlocked.Increment(ref _updatesFailed);
        }

        lock (_latencyLock)
        {
            _updateLatenciesMs.Add(latencyMs);
        }
    }

    public void RecordDelete(bool success, double latencyMs)
    {
        if (success)
        {
            Interlocked.Increment(ref _deletesCompleted);
        }
        else
        {
            Interlocked.Increment(ref _deletesFailed);
        }

        lock (_latencyLock)
        {
            _deleteLatenciesMs.Add(latencyMs);
        }
    }

    public void RecordWriteConflict()
    {
        Interlocked.Increment(ref _writeConflicts);
    }

    public void RecordRetry()
    {
        Interlocked.Increment(ref _retriesPerformed);
    }

    public void RecordMaxRetriesExceeded()
    {
        Interlocked.Increment(ref _maxRetriesExceeded);
    }

    public void RecordValidationError()
    {
        Interlocked.Increment(ref _validationErrors);
    }

    public void RecordUnexpectedError()
    {
        Interlocked.Increment(ref _unexpectedErrors);
    }

    public LatencyStats GetLatencyStats(string operationType)
    {
        List<double> latencies;

        lock (_latencyLock)
        {
            switch (operationType.ToLower())
            {
                case "insert":
                    latencies = _insertLatenciesMs.ToList();
                    break;
                case "read":
                    latencies = _readLatenciesMs.ToList();
                    break;
                case "update":
                    latencies = _updateLatenciesMs.ToList();
                    break;
                case "delete":
                    latencies = _deleteLatenciesMs.ToList();
                    break;
                default:
                    latencies = new List<double>();
                    break;
            }
        }

        LatencyStats stats = new LatencyStats();

        if (latencies.Count > 0)
        {
            latencies.Sort();

            stats.Min = latencies[0];
            stats.Max = latencies[latencies.Count - 1];
            stats.Average = latencies.Average();
            stats.P50 = GetPercentile(latencies, 50);
            stats.P95 = GetPercentile(latencies, 95);
            stats.P99 = GetPercentile(latencies, 99);
        }

        return stats;
    }

    private static double GetPercentile(List<double> sortedList, int percentile)
    {
        double result = 0;

        if (sortedList.Count > 0)
        {
            double index = (percentile / 100.0) * (sortedList.Count - 1);
            int lower = (int)Math.Floor(index);
            int upper = (int)Math.Ceiling(index);

            if (lower == upper)
            {
                result = sortedList[lower];
            }
            else
            {
                double weight = index - lower;
                result = sortedList[lower] * (1 - weight) + sortedList[upper] * weight;
            }
        }

        return result;
    }
}

public class LatencyStats
{
    public double P50 { get; set; }
    public double P95 { get; set; }
    public double P99 { get; set; }
    public double Average { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
}
