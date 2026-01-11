using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels.Customers;

[GaldrDbCollection]
public class Order
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public int CustomerId { get; set; }
    public decimal TotalAmount { get; set; }
}
