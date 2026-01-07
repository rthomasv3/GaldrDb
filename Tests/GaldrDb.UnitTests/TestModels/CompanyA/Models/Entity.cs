using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.CompanyA.Models;

[GaldrJsonSerializable]
public class Entity
{
    public int Id { get; set; }
    public string CompanyAData { get; set; }
}
