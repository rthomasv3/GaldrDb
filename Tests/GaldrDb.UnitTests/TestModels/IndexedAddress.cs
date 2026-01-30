using GaldrDbEngine.Attributes;

namespace GaldrDb.UnitTests.TestModels;

public class IndexedAddress
{
    [GaldrDbIndex]
    public string City { get; set; }

    [GaldrDbIndex(Unique = true)]
    public string ZipCode { get; set; }

    public string State { get; set; }
}
