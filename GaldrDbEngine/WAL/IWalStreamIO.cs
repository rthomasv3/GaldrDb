using System;
using System.Threading;
using System.Threading.Tasks;

namespace GaldrDbEngine.WAL;

internal interface IWalStreamIO : IDisposable
{
    int ReadAtPosition(long position, Span<byte> buffer);

    Task<int> ReadAtPositionAsync(long position, Memory<byte> buffer, CancellationToken cancellationToken = default);

    void WriteAtPosition(long position, ReadOnlySpan<byte> buffer);

    Task WriteAtPositionAsync(long position, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default);

    void Flush();

    Task FlushAsync(CancellationToken cancellationToken = default);

    long Length { get; }

    void SetLength(long length);
}
