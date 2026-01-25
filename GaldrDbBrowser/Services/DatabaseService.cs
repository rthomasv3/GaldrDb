using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using GaldrDbBrowser.Models;
using GaldrDbEngine;
using GaldrDbEngine.Json;
using GaldrDbEngine.Query;
using GaldrDbEngine.Schema;

namespace GaldrDbBrowser.Services;

public class DatabaseService : IDisposable
{
    private GaldrDb _database;
    private string _filePath;

    public bool IsOpen => _database != null;
    public string FilePath => _filePath;

    public OpenDatabaseResult OpenDatabase(string filePath)
    {
        OpenDatabaseResult result = new OpenDatabaseResult();

        try
        {
            if (_database != null)
            {
                _database.Dispose();
                _database = null;
                _filePath = null;
            }

            if (!File.Exists(filePath))
            {
                result.Success = false;
                result.Error = "File not found";
            }
            else
            {
                _database = GaldrDb.Open(filePath);
                _filePath = filePath;
                result.Success = true;
                result.FilePath = filePath;
            }
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.Error = ex.Message;
        }

        return result;
    }

    public void CloseDatabase()
    {
        if (_database != null)
        {
            _database.Dispose();
            _database = null;
            _filePath = null;
        }
    }

    public DatabaseStatsResult GetDatabaseStats()
    {
        DatabaseStatsResult result = new DatabaseStatsResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                result.Success = true;
                result.FilePath = _filePath;
                result.FileSizeBytes = new FileInfo(_filePath).Length;
                result.CollectionCount = _database.GetCollectionNames().Count;
                result.PageSize = 8192;
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    public GetCollectionsResult GetCollections()
    {
        GetCollectionsResult result = new GetCollectionsResult
        {
            Collections = new List<CollectionInfoResult>()
        };

        if (_database != null)
        {
            IReadOnlyList<string> names = _database.GetCollectionNames();

            foreach (string name in names)
            {
                CollectionInfo info = _database.GetCollectionInfo(name);
                result.Collections.Add(new CollectionInfoResult
                {
                    Name = info.Name,
                    DocumentCount = info.DocumentCount,
                    Indexes = info.Indexes.Select(idx => new IndexInfoResult
                    {
                        FieldName = idx.FieldName,
                        FieldType = idx.FieldType.ToString(),
                        IsUnique = idx.IsUnique
                    }).ToList()
                });
            }
        }

        return result;
    }

    public CollectionInfoResult GetCollectionInfo(string name)
    {
        CollectionInfoResult result = null;

        if (_database != null)
        {
            CollectionInfo info = _database.GetCollectionInfo(name);

            if (info != null)
            {
                result = new CollectionInfoResult
                {
                    Name = info.Name,
                    DocumentCount = info.DocumentCount,
                    Indexes = info.Indexes.Select(idx => new IndexInfoResult
                    {
                        FieldName = idx.FieldName,
                        FieldType = idx.FieldType.ToString(),
                        IsUnique = idx.IsUnique
                    }).ToList()
                };
            }
        }

        return result;
    }

    public async Task<QueryResult> QueryDocumentsAsync(QueryRequest request)
    {
        QueryResult result = new QueryResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                DynamicQueryBuilder countQuery = _database.QueryDynamic(request.Collection);
                DynamicQueryBuilder dataQuery = _database.QueryDynamic(request.Collection);

                if (request.Filters != null && request.Filters.Count > 0)
                {
                    foreach (FilterRequest filter in request.Filters)
                    {
                        FieldOp op = ParseFieldOp(filter.Op);
                        ApplyFilter(countQuery, filter.Field, op, filter.Value, filter.Value2);
                        ApplyFilter(dataQuery, filter.Field, op, filter.Value, filter.Value2);
                    }
                }

                int totalCount = await countQuery.CountAsync();

                string orderByField = string.IsNullOrEmpty(request.OrderByField) ? "Id" : request.OrderByField;
                if (request.OrderByDescending)
                {
                    dataQuery.OrderByDescending(orderByField);
                }
                else
                {
                    dataQuery.OrderBy(orderByField);
                }

                List<JsonDocument> docs = await dataQuery
                    .Skip(request.Skip)
                    .Limit(request.Limit)
                    .ToListAsync();

                result.Success = true;
                result.TotalCount = totalCount;
                result.Skip = request.Skip;
                result.Limit = request.Limit;
                result.HasMore = request.Skip + docs.Count < totalCount;
                result.Documents = docs.Select(doc => new DocumentResult
                {
                    Id = doc.GetInt32("Id"),
                    Json = doc.ToJsonString()
                }).ToList();
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    private static FieldOp ParseFieldOp(string op)
    {
        FieldOp result = op switch
        {
            "eq" => FieldOp.Equals,
            "neq" => FieldOp.NotEquals,
            "gt" => FieldOp.GreaterThan,
            "gte" => FieldOp.GreaterThanOrEqual,
            "lt" => FieldOp.LessThan,
            "lte" => FieldOp.LessThanOrEqual,
            "startsWith" => FieldOp.StartsWith,
            "endsWith" => FieldOp.EndsWith,
            "contains" => FieldOp.Contains,
            "between" => FieldOp.Between,
            _ => FieldOp.Equals
        };

        return result;
    }

    private static void ApplyFilter(DynamicQueryBuilder query, string field, FieldOp op, string value, string value2)
    {
        if (op == FieldOp.Between && value2 != null)
        {
            if (int.TryParse(value, out int intMin) && int.TryParse(value2, out int intMax))
            {
                query.WhereBetween(field, intMin, intMax);
            }
            else if (double.TryParse(value, out double doubleMin) && double.TryParse(value2, out double doubleMax))
            {
                query.WhereBetween(field, doubleMin, doubleMax);
            }
            else
            {
                query.WhereBetween(field, value, value2);
            }
        }
        else if (int.TryParse(value, out int intValue))
        {
            query.Where(field, op, intValue);
        }
        else if (double.TryParse(value, out double doubleValue))
        {
            query.Where(field, op, doubleValue);
        }
        else if (bool.TryParse(value, out bool boolValue))
        {
            query.Where(field, op, boolValue);
        }
        else
        {
            query.Where(field, op, value);
        }
    }

    public async Task<GetDocumentResult> GetDocumentAsync(string collection, int id)
    {
        GetDocumentResult result = new GetDocumentResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                JsonDocument doc = await _database.GetByIdDynamicAsync(collection, id);

                if (doc != null)
                {
                    result.Success = true;
                    result.Id = id;
                    result.Json = doc.ToJsonString();
                }
                else
                {
                    result.Success = false;
                    result.Error = "Document not found";
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    public async Task<MutationResult> InsertDocumentAsync(InsertDocumentRequest request)
    {
        MutationResult result = new MutationResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                int id = await _database.InsertDynamicAsync(request.Collection, request.Json);
                result.Success = true;
                result.Id = id;
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    public async Task<MutationResult> ReplaceDocumentAsync(ReplaceDocumentRequest request)
    {
        MutationResult result = new MutationResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                bool replaced = await _database.ReplaceDynamicAsync(request.Collection, request.Id, request.Json);

                if (replaced)
                {
                    result.Success = true;
                    result.Id = request.Id;
                }
                else
                {
                    result.Success = false;
                    result.Error = "Document not found";
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    public async Task<MutationResult> DeleteDocumentAsync(string collection, int id)
    {
        MutationResult result = new MutationResult();

        if (_database == null)
        {
            result.Success = false;
            result.Error = "No database open";
        }
        else
        {
            try
            {
                bool deleted = await _database.DeleteByIdDynamicAsync(collection, id);

                if (deleted)
                {
                    result.Success = true;
                    result.Id = id;
                }
                else
                {
                    result.Success = false;
                    result.Error = "Document not found";
                }
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }
        }

        return result;
    }

    public void Dispose()
    {
        CloseDatabase();
    }
}
