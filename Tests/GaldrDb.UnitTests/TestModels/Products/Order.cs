using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.Products;

[GaldrDbCollection]
public class Order
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public int ProductId { get; set; }
    public int Quantity { get; set; }
}
