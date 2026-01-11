using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.CompanyA.Models;

[GaldrDbCollection]
public class Entity
{
    public int Id { get; set; }
    public string CompanyAData { get; set; }
}
