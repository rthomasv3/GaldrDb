using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDbConsole.Models;

[GaldrDbCollection]
public class BenchmarkPersonNoIndex
{
    public int Id { get; set; }

    public string Name { get; set; }

    public int Age { get; set; }

    public string Email { get; set; }

    public string Address { get; set; }

    public string Phone { get; set; }
}
