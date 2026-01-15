using System.Buffers;

namespace GaldrDbEngine.Utilities;

public static class IntArrayPool
{
    public static int[] Rent(int minimumSize)
    {
        return ArrayPool<int>.Shared.Rent(minimumSize);
    }

    public static void Return(int[] array, bool clearArray = false)
    {
        if (array != null)
        {
            ArrayPool<int>.Shared.Return(array, clearArray);
        }
    }
}
