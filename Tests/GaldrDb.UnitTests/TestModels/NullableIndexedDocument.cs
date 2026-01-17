using System;
using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
public class NullableIndexedDocument
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public int? NullableInt { get; set; }
    [GaldrDbIndex]
    public short? NullableInt16 { get; set; }
    [GaldrDbIndex]
    public float? NullableSingle { get; set; }
    [GaldrDbIndex]
    public TimeSpan? NullableTimeSpan { get; set; }
    [GaldrDbIndex]
    public Priority? NullablePriority { get; set; }
    public string Name { get; set; }
}
