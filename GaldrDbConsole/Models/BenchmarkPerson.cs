using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDbConsole.Models;

[GaldrJsonSerializable]
public class BenchmarkPerson
{
    public int Id { get; set; }

    [GaldrIndex]
    public string Name { get; set; }

    public int Age { get; set; }

    public string Email { get; set; }

    public string Address { get; set; }

    public string Phone { get; set; }
}
