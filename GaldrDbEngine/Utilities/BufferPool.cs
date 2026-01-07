using System;
using System.Buffers;

namespace GaldrDbEngine.Utilities;

public static class BufferPool
{
    public static byte[] Rent(int minimumSize)
    {
        return ArrayPool<byte>.Shared.Rent(minimumSize);
    }

    public static void Return(byte[] buffer, bool clearArray = false)
    {
        ArrayPool<byte>.Shared.Return(buffer, clearArray);
    }
}
