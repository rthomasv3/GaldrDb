using System;

namespace GaldrDbEngine.Utilities;

public readonly struct KeyBuffer
{
    public readonly byte[] Data;
    public readonly int Offset;
    public readonly int Length;

    public KeyBuffer(byte[] data, int offset, int length)
    {
        Data = data;
        Offset = offset;
        Length = length;
    }

    public static KeyBuffer FromCopy(byte[] source, int sourceOffset, int length)
    {
        byte[] data = new byte[length];
        Array.Copy(source, sourceOffset, data, 0, length);
        return new KeyBuffer(data, 0, length);
    }

    public ReadOnlySpan<byte> AsSpan()
    {
        return new ReadOnlySpan<byte>(Data, Offset, Length);
    }

    public static int Compare(KeyBuffer a, KeyBuffer b)
    {
        return Compare(a.AsSpan(), b.AsSpan());
    }

    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int result = 0;
        int minLength = Math.Min(a.Length, b.Length);
        int i = 0;

        while (i < minLength && result == 0)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                result = diff;
            }
            i++;
        }

        if (result == 0)
        {
            result = a.Length - b.Length;
        }

        return result;
    }

    public static int Compare(ReadOnlySpan<byte> a, KeyBuffer b)
    {
        return Compare(a, b.AsSpan());
    }

    public bool StartsWith(byte[] prefix)
    {
        bool result = true;

        if (Length < prefix.Length)
        {
            result = false;
        }
        else
        {
            int i = 0;
            while (i < prefix.Length && result)
            {
                if (Data[Offset + i] != prefix[i])
                {
                    result = false;
                }
                i++;
            }
        }

        return result;
    }
}
