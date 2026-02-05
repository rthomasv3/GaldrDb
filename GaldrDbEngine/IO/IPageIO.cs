using System;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.IO;

internal interface IPageIO : IDisposable
{
    void ReadPage(int pageId, Span<byte> destination, TransactionContext context = null);
    void WritePage(int pageId, ReadOnlySpan<byte> data, TransactionContext context = null);
    void Flush();
    void Close();

    /// <summary>
    /// Extends the file to the specified size. Used for file expansion without writing data.
    /// The new space may be zero-filled by the OS or contain undefined data depending on platform.
    /// </summary>
    void SetLength(long newSize);

    Task ReadPageAsync(int pageId, Memory<byte> destination, TransactionContext context = null, CancellationToken cancellationToken = default);
    Task WritePageAsync(int pageId, ReadOnlyMemory<byte> data, TransactionContext context = null, CancellationToken cancellationToken = default);
    Task FlushAsync(CancellationToken cancellationToken = default);
}
