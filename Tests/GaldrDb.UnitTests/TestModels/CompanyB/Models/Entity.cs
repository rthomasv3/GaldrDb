using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.CompanyB.Models;

[GaldrJsonSerializable]
public class Entity
{
    public int Id { get; set; }
    public string CompanyBData { get; set; }
}
