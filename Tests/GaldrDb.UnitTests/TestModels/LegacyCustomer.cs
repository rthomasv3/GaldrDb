using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

/// <summary>
/// This class was renamed from "Customer" but needs to maintain compatibility
/// with the existing "Customer" collection in the database.
/// </summary>
[GaldrJsonSerializable]
[GaldrCollection("Customer")]
public class LegacyCustomer
{
    public int Id { get; set; }
    [GaldrIndex]
    public string Name { get; set; }
    public string Email { get; set; }
}
