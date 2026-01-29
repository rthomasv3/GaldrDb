using System.Collections.Generic;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
public class PersonWithAddress
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Address Address { get; set; }
    public List<Address> PreviousAddresses { get; set; }
}
