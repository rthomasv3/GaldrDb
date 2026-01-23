using System.Collections.Generic;

namespace GaldrDbBrowser.Models;

public class QueryResult
{
    public bool Success { get; set; }
    public string Error { get; set; }
    public List<DocumentResult> Documents { get; set; }
    public int TotalCount { get; set; }
    public int Skip { get; set; }
    public int Limit { get; set; }
    public bool HasMore { get; set; }
}
