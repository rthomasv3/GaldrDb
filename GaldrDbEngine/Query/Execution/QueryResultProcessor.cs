using System.Collections.Generic;
using GaldrDbEngine.Json;

namespace GaldrDbEngine.Query.Execution;

internal static class QueryResultProcessor
{
    public static List<T> ApplyOrdering<T>(List<T> documents, IReadOnlyList<OrderByClause<T>> orderByClauses)
    {
        List<T> result;

        if (orderByClauses.Count == 0)
        {
            result = documents;
        }
        else
        {
            result = new List<T>(documents);
            result.Sort((a, b) =>
            {
                int cmp = 0;

                foreach (OrderByClause<T> clause in orderByClauses)
                {
                    cmp = clause.Comparer(a, b);
                    if (cmp != 0)
                    {
                        break;
                    }
                }

                return cmp;
            });
        }

        return result;
    }

    public static List<JsonDocument> ApplyDynamicOrdering(List<JsonDocument> documents, IReadOnlyList<DynamicOrderByClause> orderByClauses)
    {
        List<JsonDocument> result;

        if (orderByClauses.Count == 0)
        {
            result = documents;
        }
        else
        {
            result = new List<JsonDocument>(documents);
            result.Sort((a, b) =>
            {
                int cmp = 0;

                foreach (DynamicOrderByClause clause in orderByClauses)
                {
                    cmp = clause.Compare(a, b);
                    if (cmp != 0)
                    {
                        break;
                    }
                }

                return cmp;
            });
        }

        return result;
    }

    public static List<T> ApplySkipAndLimit<T>(List<T> documents, int? skip, int? limit)
    {
        int skipCount = skip ?? 0;
        int startIndex = skipCount;
        List<T> results;

        if (startIndex >= documents.Count)
        {
            results = new List<T>();
        }
        else
        {
            int takeCount;
            if (limit.HasValue)
            {
                takeCount = limit.Value;
            }
            else
            {
                takeCount = documents.Count - startIndex;
            }

            int actualCount = takeCount;
            if (startIndex + takeCount > documents.Count)
            {
                actualCount = documents.Count - startIndex;
            }

            results = new List<T>(actualCount);
            for (int i = 0; i < actualCount; i++)
            {
                results.Add(documents[startIndex + i]);
            }
        }

        return results;
    }
}
