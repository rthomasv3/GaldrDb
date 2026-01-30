using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
[GaldrDbCompoundIndex("Status", "Address.City")]
public class PersonWithIndexedAddress
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Status { get; set; }
    public IndexedAddress Address { get; set; }
}
