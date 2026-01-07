using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.Customers;

[GaldrJsonSerializable]
public class Order
{
    public int Id { get; set; }
    [GaldrIndex]
    public int CustomerId { get; set; }
    public decimal TotalAmount { get; set; }
}
