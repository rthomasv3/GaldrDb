using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.Products;

[GaldrJsonSerializable]
public class Order
{
    public int Id { get; set; }
    [GaldrIndex]
    public int ProductId { get; set; }
    public int Quantity { get; set; }
}
