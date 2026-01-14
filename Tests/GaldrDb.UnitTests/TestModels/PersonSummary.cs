using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbProjection(typeof(Person))]
public partial class PersonSummary
{
    public int Id { get; set; }
    public string Name { get; set; }
}
