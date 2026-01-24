namespace GaldrDbBrowser.Models;

public class ReplaceDocumentRequest
{
    public string Collection { get; set; }
    public int Id { get; set; }
    public string Json { get; set; }
}
