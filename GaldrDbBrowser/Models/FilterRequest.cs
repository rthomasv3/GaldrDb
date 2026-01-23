namespace GaldrDbBrowser.Models;

public class FilterRequest
{
    public string Field { get; set; }
    public string Op { get; set; }
    public string Value { get; set; }
    public string Value2 { get; set; }
}
