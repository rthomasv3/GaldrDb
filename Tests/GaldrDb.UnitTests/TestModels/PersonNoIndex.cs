using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
public class PersonNoIndex
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
    public string Email { get; set; }
}