using System.Collections.Generic;

namespace GaldrDbBrowser.Models;

public class QueryRequest
{
    public string Collection { get; set; }
    public int Skip { get; set; }
    public int Limit { get; set; } = 50;
    public List<FilterRequest> Filters { get; set; }
    public string OrderByField { get; set; }
    public bool OrderByDescending { get; set; }
}
