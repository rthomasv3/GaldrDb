using System;
using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
public class IndexedTypesDocument
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public byte ByteField { get; set; }
    [GaldrDbIndex]
    public sbyte SByteField { get; set; }
    [GaldrDbIndex]
    public short Int16Field { get; set; }
    [GaldrDbIndex]
    public ushort UInt16Field { get; set; }
    [GaldrDbIndex]
    public uint UInt32Field { get; set; }
    [GaldrDbIndex]
    public ulong UInt64Field { get; set; }
    [GaldrDbIndex]
    public float SingleField { get; set; }
    [GaldrDbIndex]
    public char CharField { get; set; }
    [GaldrDbIndex]
    public TimeSpan TimeSpanField { get; set; }
    [GaldrDbIndex]
    public Priority PriorityField { get; set; }
}
