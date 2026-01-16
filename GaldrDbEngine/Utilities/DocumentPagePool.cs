using System.Collections.Concurrent;
using GaldrDbEngine.Pages;

namespace GaldrDbEngine.Utilities;

internal static class DocumentPagePool
{
    private static readonly ConcurrentDictionary<int, ConcurrentBag<DocumentPage>> _pools = new();

    public static DocumentPage Rent(int pageSize)
    {
        DocumentPage result;
        ConcurrentBag<DocumentPage> pool = _pools.GetOrAdd(pageSize, _ => new ConcurrentBag<DocumentPage>());

        if (pool.TryTake(out DocumentPage page))
        {
            result = page;
        }
        else
        {
            result = new DocumentPage();
        }

        return result;
    }

    public static void Return(DocumentPage page)
    {
        if (page != null)
        {
            int pageSize = page.PageData?.Length ?? 0;
            if (pageSize > 0)
            {
                ConcurrentBag<DocumentPage> pool = _pools.GetOrAdd(pageSize, _ => new ConcurrentBag<DocumentPage>());
                pool.Add(page);
            }
        }
    }

    public static void Warmup(int pageSize, int count)
    {
        ConcurrentBag<DocumentPage> pool = _pools.GetOrAdd(pageSize, _ => new ConcurrentBag<DocumentPage>());

        for (int i = 0; i < count; i++)
        {
            DocumentPage page = DocumentPage.CreateNew(pageSize);
            pool.Add(page);
        }
    }
}
