using System;

namespace GaldrDbEngine.IO;

public interface IPageIO : IDisposable
{
    void ReadPage(int pageId, Span<byte> destination);
    void WritePage(int pageId, ReadOnlySpan<byte> data);
    void Flush();
    void Close();
}
