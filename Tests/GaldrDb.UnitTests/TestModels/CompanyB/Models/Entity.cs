using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.CompanyB.Models;

[GaldrDbCollection]
public class Entity
{
    public int Id { get; set; }
    public string CompanyBData { get; set; }
}
