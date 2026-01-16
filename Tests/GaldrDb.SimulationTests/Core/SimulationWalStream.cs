using System;
using System.IO;

namespace GaldrDb.SimulationTests.Core;

public class SimulationWalStream : MemoryStream
{
    private byte[] _persistedData;
    private long _persistedLength;
    private readonly SimulationStats _stats;
    private FaultInjector _faultInjector;

    public SimulationWalStream(SimulationStats stats)
    {
        _persistedData = Array.Empty<byte>();
        _persistedLength = 0;
        _stats = stats;
        _faultInjector = null;
    }

    /// <summary>
    /// Sets the fault injector for this WAL stream.
    /// </summary>
    public void SetFaultInjector(FaultInjector faultInjector)
    {
        _faultInjector = faultInjector;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        _stats.WalReads++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckReadFault(-1);

            if (fault == FaultType.ReadError)
            {
                throw new IOException("Simulated WAL read error");
            }

            if (fault == FaultType.CorruptRead)
            {
                int bytesRead = base.Read(buffer, offset, count);
                if (bytesRead > 0)
                {
                    _faultInjector.CorruptData(buffer.AsSpan(offset, bytesRead));
                }
                return bytesRead;
            }
        }

        return base.Read(buffer, offset, count);
    }

    public override int Read(Span<byte> buffer)
    {
        _stats.WalReads++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckReadFault(-1);

            if (fault == FaultType.ReadError)
            {
                throw new IOException("Simulated WAL read error");
            }

            if (fault == FaultType.CorruptRead)
            {
                int bytesRead = base.Read(buffer);
                if (bytesRead > 0)
                {
                    _faultInjector.CorruptData(buffer.Slice(0, bytesRead));
                }
                return bytesRead;
            }
        }

        return base.Read(buffer);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _stats.WalWrites++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckWriteFault(-1);

            if (fault == FaultType.WriteError)
            {
                throw new IOException("Simulated WAL write error");
            }

            if (fault == FaultType.PartialWrite)
            {
                // Only write part of the data
                int partialCount = _faultInjector.GetPartialWriteSize(count);
                base.Write(buffer, offset, Math.Min(partialCount, count));
                return;
            }
        }

        base.Write(buffer, offset, count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        _stats.WalWrites++;

        // Check for fault injection
        if (_faultInjector != null)
        {
            FaultType fault = _faultInjector.CheckWriteFault(-1);

            if (fault == FaultType.WriteError)
            {
                throw new IOException("Simulated WAL write error");
            }

            if (fault == FaultType.PartialWrite)
            {
                // Only write part of the data
                int partialCount = _faultInjector.GetPartialWriteSize(buffer.Length);
                base.Write(buffer.Slice(0, Math.Min(partialCount, buffer.Length)));
                return;
            }
        }

        base.Write(buffer);
    }

    public override void Flush()
    {
        _stats.WalFlushes++;

        // Snapshot current state as persisted
        _persistedData = ToArray();
        _persistedLength = Length;

        base.Flush();
    }

    public override void Close()
    {
        // Don't actually close - persist state and allow reuse
        _persistedData = ToArray();
        _persistedLength = Length;
        // Don't call base.Close() - we want to keep the stream usable
    }

    protected override void Dispose(bool disposing)
    {
        // Don't actually dispose - persist state and allow reuse
        _persistedData = ToArray();
        _persistedLength = Length;
        // Don't call base.Dispose() - we want to keep the stream usable
    }

    /// <summary>
    /// Simulates a crash by discarding all unflushed writes
    /// and restoring to the last flushed state.
    /// </summary>
    public void SimulateCrash()
    {
        // Note: CrashCount is incremented by SimulationPageIO to avoid double-counting
        // when both pageIO and walStream crash together

        // Clear current buffer and restore persisted state
        SetLength(0);
        if (_persistedLength > 0)
        {
            base.Write(_persistedData, 0, (int)_persistedLength);
        }
        Position = 0;
    }

    /// <summary>
    /// Gets the persisted length (length at last flush).
    /// </summary>
    public long PersistedLength => _persistedLength;

    /// <summary>
    /// Returns true if there are unflushed writes.
    /// </summary>
    public bool HasUnflushedWrites => Length != _persistedLength || !DataMatches();

    private bool DataMatches()
    {
        if (Length != _persistedLength)
        {
            return false;
        }

        byte[] currentData = ToArray();
        for (int i = 0; i < _persistedLength; i++)
        {
            if (currentData[i] != _persistedData[i])
            {
                return false;
            }
        }

        return true;
    }
}
