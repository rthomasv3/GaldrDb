using System;
using System.Text.Json;

namespace GaldrDbEngine.Utilities;

internal sealed class PooledJsonWriter
{
    private readonly PooledBufferWriter _bufferWriter;
    private readonly Utf8JsonWriter _writer;
    private readonly int _initialCapacity;

    public Utf8JsonWriter Writer => _writer;

    public ReadOnlySpan<byte> WrittenSpan => _bufferWriter.WrittenSpan;

    public int WrittenCount => _bufferWriter.WrittenCount;

    public int BufferCapacity => _bufferWriter.Capacity;

    public PooledJsonWriter(int bufferCapacity)
    {
        _initialCapacity = bufferCapacity;
        _bufferWriter = new PooledBufferWriter(bufferCapacity);
        _writer = new Utf8JsonWriter(_bufferWriter, new JsonWriterOptions { Indented = false });
    }

    public void Reset()
    {
        _bufferWriter.Reset();
        _writer.Reset(_bufferWriter);
    }

    public void PrepareForReturn()
    {
        _bufferWriter.ShrinkIfOversized();
    }
}
