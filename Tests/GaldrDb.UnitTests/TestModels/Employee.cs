using System;
using GaldrDbEngine.Attributes;
using GaldrJson;

namespace GaldrDb.UnitTests.TestModels;

[GaldrDbCollection]
[GaldrDbCompoundIndex("Department", "EmployeeNumber", Unique = true)]
[GaldrDbCompoundIndex("Department", "HireDate")]
public class Employee
{
    public int Id { get; set; }
    public string Name { get; set; }
    public string Department { get; set; }
    public string EmployeeNumber { get; set; }
    public DateTime HireDate { get; set; }
}
