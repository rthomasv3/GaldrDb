using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.WAL;

namespace GaldrDb.SimulationTests.Core;

public class SimulationWalStreamIO : IWalStreamIO
{
    private readonly SimulationWalStream _stream;
    private readonly object _positionLock;

    public SimulationWalStreamIO(SimulationWalStream stream)
    {
        _stream = stream;
        _positionLock = new object();
    }

    public SimulationWalStream UnderlyingStream => _stream;

    public long Length
    {
        get
        {
            lock (_positionLock)
            {
                return _stream.Length;
            }
        }
    }

    public int ReadAtPosition(long position, Span<byte> buffer)
    {
        lock (_positionLock)
        {
            _stream.Position = position;
            return _stream.Read(buffer);
        }
    }

    public Task<int> ReadAtPositionAsync(long position, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int bytesRead = ReadAtPosition(position, buffer.Span);
        return Task.FromResult(bytesRead);
    }

    public void WriteAtPosition(long position, ReadOnlySpan<byte> buffer)
    {
        lock (_positionLock)
        {
            _stream.Position = position;
            _stream.Write(buffer);
        }
    }

    public Task WriteAtPositionAsync(long position, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        WriteAtPosition(position, buffer.Span);
        return Task.CompletedTask;
    }

    public void Flush()
    {
        lock (_positionLock)
        {
            _stream.Flush();
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    public void SetLength(long length)
    {
        lock (_positionLock)
        {
            _stream.SetLength(length);
        }
    }

    public void Dispose()
    {
        // Don't dispose underlying stream - SimulationWalStream manages its own lifecycle
    }
}
