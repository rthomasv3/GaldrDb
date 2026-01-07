using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrJsonSerializable]
public class Person
{
    public string Name { get; set; }
    public int Age { get; set; }
    public string Email { get; set; }
}
