using System;

namespace GaldrDbCore.IO;

public interface IPageIO : IDisposable
{
    byte[] ReadPage(int pageId);
    void WritePage(int pageId, byte[] data);
    void Flush();
    void Close();
}
