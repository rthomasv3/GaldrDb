using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
public class User
{
    public int Id { get; set; }
    public string Name { get; set; }
    [GaldrDbIndex(Unique = true)]
    public string Email { get; set; }
    [GaldrDbIndex]
    public string Department { get; set; }
}
