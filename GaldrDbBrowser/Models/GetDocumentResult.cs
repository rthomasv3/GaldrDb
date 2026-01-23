namespace GaldrDbBrowser.Models;

public class GetDocumentResult
{
    public bool Success { get; set; }
    public string Error { get; set; }
    public int Id { get; set; }
    public string Json { get; set; }
}
