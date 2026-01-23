using System.Collections.Generic;

namespace GaldrDbBrowser.Models;

public class CollectionInfoResult
{
    public string Name { get; set; }
    public int DocumentCount { get; set; }
    public List<IndexInfoResult> Indexes { get; set; }
}

public class IndexInfoResult
{
    public string FieldName { get; set; }
    public string FieldType { get; set; }
    public bool IsUnique { get; set; }
}
