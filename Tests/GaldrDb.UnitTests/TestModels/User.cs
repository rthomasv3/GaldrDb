using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrJsonSerializable]
public class User
{
    public int Id { get; set; }
    public string Name { get; set; }
    [GaldrIndex(Unique = true)]
    public string Email { get; set; }
    [GaldrIndex]
    public string Department { get; set; }
}
