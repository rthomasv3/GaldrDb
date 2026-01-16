using System;
using System.Buffers;

namespace GaldrDbEngine.Utilities;

internal sealed class PooledBufferWriter : IBufferWriter<byte>
{
    private const int DEFAULT_SIZE_HINT = 256;

    private byte[] _buffer;
    private int _written;
    private readonly int _initialCapacity;

    public int WrittenCount => _written;

    public int Capacity => _buffer.Length;

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

    public PooledBufferWriter(int initialCapacity)
    {
        if (initialCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));
        }

        _initialCapacity = initialCapacity;
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        _written = 0;
    }

    public void Advance(int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (_written > _buffer.Length - count)
        {
            throw new InvalidOperationException("Cannot advance past the end of the buffer.");
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    public void Reset()
    {
        _written = 0;
    }

    public void ShrinkIfOversized()
    {
        if (_buffer.Length > _initialCapacity * 4)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(_initialCapacity);
        }
    }

    public void Dispose()
    {
        if (_buffer != null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
        }
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint <= 0)
        {
            sizeHint = DEFAULT_SIZE_HINT;
        }

        int available = _buffer.Length - _written;

        if (available < sizeHint)
        {
            int needed = _written + sizeHint;
            int newSize = _buffer.Length;

            while (newSize < needed)
            {
                newSize *= 2;
            }

            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            _buffer.AsSpan(0, _written).CopyTo(newBuffer);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }
    }
}
