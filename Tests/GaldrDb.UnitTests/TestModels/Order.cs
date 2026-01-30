using System;
using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
[GaldrDbCompoundIndex("Status", "CreatedDate")]
[GaldrDbCompoundIndex("Category", "Priority")]
[GaldrDbCompoundIndex("Status", "Category", "Priority")]
public class Order
{
    public int Id { get; set; }
    public string Status { get; set; }
    public DateTime CreatedDate { get; set; }
    public string Category { get; set; }
    public int Priority { get; set; }
    public decimal Amount { get; set; }
    public string CustomerName { get; set; }
}
