using System;
using System.Collections.Generic;

namespace GaldrDb.SimulationTests.Core;

/// <summary>
/// Types of faults that can be injected during simulation.
/// </summary>
public enum FaultType
{
    None,
    ReadError,
    WriteError,
    PartialWrite,
    CorruptRead
}

/// <summary>
/// Configuration for a specific fault injection.
/// </summary>
public class FaultConfig
{
    public FaultType Type { get; set; }
    public double Probability { get; set; }
    public int? TargetPageId { get; set; }
    public int? CorruptionOffset { get; set; }
    public int? CorruptionLength { get; set; }
    public int? PartialWriteBytes { get; set; }
}

/// <summary>
/// Controls fault injection during simulation testing.
/// Faults can be injected probabilistically or deterministically.
/// </summary>
public class FaultInjector
{
    private readonly SimulationRandom _rng;
    private readonly SimulationStats _stats;
    private readonly List<FaultConfig> _scheduledFaults;
    private readonly object _lock = new object();

    private double _readErrorProbability;
    private double _writeErrorProbability;
    private double _partialWriteProbability;
    private double _corruptReadProbability;

    private bool _enabled;
    private int _faultCountdown;
    private FaultType _nextFaultType;

    public FaultInjector(SimulationRandom rng, SimulationStats stats)
    {
        _rng = rng;
        _stats = stats;
        _scheduledFaults = new List<FaultConfig>();
        _enabled = false;
        _faultCountdown = -1;
        _nextFaultType = FaultType.None;
    }

    public bool Enabled
    {
        get { return _enabled; }
        set { _enabled = value; }
    }

    /// <summary>
    /// Sets the probability of read errors (0.0 to 1.0).
    /// </summary>
    public void SetReadErrorProbability(double probability)
    {
        _readErrorProbability = Math.Clamp(probability, 0.0, 1.0);
    }

    /// <summary>
    /// Sets the probability of write errors (0.0 to 1.0).
    /// </summary>
    public void SetWriteErrorProbability(double probability)
    {
        _writeErrorProbability = Math.Clamp(probability, 0.0, 1.0);
    }

    /// <summary>
    /// Sets the probability of partial writes (0.0 to 1.0).
    /// </summary>
    public void SetPartialWriteProbability(double probability)
    {
        _partialWriteProbability = Math.Clamp(probability, 0.0, 1.0);
    }

    /// <summary>
    /// Sets the probability of corrupt reads (0.0 to 1.0).
    /// </summary>
    public void SetCorruptReadProbability(double probability)
    {
        _corruptReadProbability = Math.Clamp(probability, 0.0, 1.0);
    }

    /// <summary>
    /// Schedules a specific fault to occur after a certain number of operations.
    /// </summary>
    public void ScheduleFaultAfter(int operationCount, FaultType faultType)
    {
        lock (_lock)
        {
            _faultCountdown = operationCount;
            _nextFaultType = faultType;
        }
    }

    /// <summary>
    /// Adds a scheduled fault configuration.
    /// </summary>
    public void AddScheduledFault(FaultConfig config)
    {
        lock (_lock)
        {
            _scheduledFaults.Add(config);
        }
    }

    /// <summary>
    /// Clears all scheduled faults.
    /// </summary>
    public void ClearScheduledFaults()
    {
        lock (_lock)
        {
            _scheduledFaults.Clear();
            _faultCountdown = -1;
            _nextFaultType = FaultType.None;
        }
    }

    /// <summary>
    /// Checks if a read fault should occur and returns the fault type.
    /// </summary>
    public FaultType CheckReadFault(int pageId)
    {
        if (!_enabled)
        {
            return FaultType.None;
        }

        lock (_lock)
        {
            // Check scheduled fault
            if (_faultCountdown == 0 && (_nextFaultType == FaultType.ReadError || _nextFaultType == FaultType.CorruptRead))
            {
                FaultType fault = _nextFaultType;
                _faultCountdown = -1;
                _nextFaultType = FaultType.None;
                RecordFault(fault);
                return fault;
            }

            if (_faultCountdown > 0)
            {
                _faultCountdown--;
            }

            // Check probabilistic faults
            if (_readErrorProbability > 0 && _rng.NextDouble() < _readErrorProbability)
            {
                RecordFault(FaultType.ReadError);
                return FaultType.ReadError;
            }

            if (_corruptReadProbability > 0 && _rng.NextDouble() < _corruptReadProbability)
            {
                RecordFault(FaultType.CorruptRead);
                return FaultType.CorruptRead;
            }
        }

        return FaultType.None;
    }

    /// <summary>
    /// Checks if a write fault should occur and returns the fault type.
    /// </summary>
    public FaultType CheckWriteFault(int pageId)
    {
        if (!_enabled)
        {
            return FaultType.None;
        }

        lock (_lock)
        {
            // Check scheduled fault
            if (_faultCountdown == 0 && (_nextFaultType == FaultType.WriteError || _nextFaultType == FaultType.PartialWrite))
            {
                FaultType fault = _nextFaultType;
                _faultCountdown = -1;
                _nextFaultType = FaultType.None;
                RecordFault(fault);
                return fault;
            }

            if (_faultCountdown > 0)
            {
                _faultCountdown--;
            }

            // Check probabilistic faults
            if (_writeErrorProbability > 0 && _rng.NextDouble() < _writeErrorProbability)
            {
                RecordFault(FaultType.WriteError);
                return FaultType.WriteError;
            }

            if (_partialWriteProbability > 0 && _rng.NextDouble() < _partialWriteProbability)
            {
                RecordFault(FaultType.PartialWrite);
                return FaultType.PartialWrite;
            }
        }

        return FaultType.None;
    }

    /// <summary>
    /// Corrupts data by flipping random bits.
    /// </summary>
    public void CorruptData(Span<byte> data)
    {
        if (data.Length == 0)
        {
            return;
        }

        // Corrupt 1-8 bytes at random positions
        int corruptionCount = _rng.Next(1, 9);

        for (int i = 0; i < corruptionCount; i++)
        {
            int position = _rng.Next(0, data.Length);
            data[position] = (byte)(data[position] ^ _rng.Next(1, 256));
        }
    }

    /// <summary>
    /// Gets the number of bytes to write for a partial write fault.
    /// </summary>
    public int GetPartialWriteSize(int fullSize)
    {
        // Write between 1 byte and fullSize-1 bytes
        int partialSize = _rng.Next(1, fullSize);
        return partialSize;
    }

    private void RecordFault(FaultType faultType)
    {
        _stats.FaultsInjected++;

        switch (faultType)
        {
            case FaultType.ReadError:
                _stats.ReadErrorsInjected++;
                break;
            case FaultType.WriteError:
                _stats.WriteErrorsInjected++;
                break;
            case FaultType.PartialWrite:
                _stats.PartialWritesInjected++;
                break;
            case FaultType.CorruptRead:
                _stats.CorruptReadsInjected++;
                break;
        }
    }
}
