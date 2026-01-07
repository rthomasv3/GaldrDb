using System.Collections.Generic;

namespace GaldrDbCore.Query;

public interface IQueryExecutor<T>
{
    List<T> ExecuteQuery(QueryBuilder<T> query);
    int ExecuteCount(QueryBuilder<T> query);
}
