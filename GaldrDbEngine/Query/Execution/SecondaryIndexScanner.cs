using System.Collections.Generic;
using GaldrDbEngine.Query.Planning;
using GaldrDbEngine.Storage;

namespace GaldrDbEngine.Query.Execution;

internal sealed class SecondaryIndexScanner
{
    private readonly GaldrDb _db;

    public SecondaryIndexScanner(GaldrDb db)
    {
        _db = db;
    }

    public List<SecondaryIndexEntry> GetEntries(QueryPlan plan)
    {
        return GetEntriesCore(plan.IndexDefinition, plan.IndexFilter);
    }

    public List<SecondaryIndexEntry> GetEntries(SecondaryIndexSpec indexSpec)
    {
        return GetEntriesCore(indexSpec.IndexDefinition, indexSpec.IndexFilter);
    }

    private List<SecondaryIndexEntry> GetEntriesCore(IndexDefinition indexDef, IFieldFilter filter)
    {
        List<SecondaryIndexEntry> entries;
        byte[] keyBytes = filter.GetIndexKeyBytes();

        if (filter.Operation == FieldOp.Equals)
        {
            entries = _db.SearchSecondaryIndexExact(indexDef, keyBytes);
        }
        else if (filter.Operation == FieldOp.StartsWith)
        {
            entries = _db.SearchSecondaryIndex(indexDef, keyBytes);
        }
        else if (filter.Operation == FieldOp.In)
        {
            entries = new List<SecondaryIndexEntry>();
            HashSet<int> seenDocIds = new HashSet<int>();
            foreach (byte[] valueKeyBytes in filter.GetAllIndexKeyBytes())
            {
                List<SecondaryIndexEntry> valueEntries = _db.SearchSecondaryIndexExact(indexDef, valueKeyBytes);
                foreach (SecondaryIndexEntry entry in valueEntries)
                {
                    if (seenDocIds.Add(entry.DocId))
                    {
                        entries.Add(entry);
                    }
                }
            }
        }
        else if (filter.Operation == FieldOp.Between)
        {
            byte[] endKeyBytes = filter.GetIndexKeyEndBytes();
            entries = _db.SearchSecondaryIndexRange(indexDef, keyBytes, endKeyBytes, true, true);
        }
        else if (filter.Operation == FieldOp.GreaterThan)
        {
            entries = _db.SearchSecondaryIndexRange(indexDef, keyBytes, null, false, true);
        }
        else if (filter.Operation == FieldOp.GreaterThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(indexDef, keyBytes, null, true, true);
        }
        else if (filter.Operation == FieldOp.LessThan)
        {
            entries = _db.SearchSecondaryIndexRange(indexDef, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, false);
        }
        else if (filter.Operation == FieldOp.LessThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(indexDef, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, true);
        }
        else
        {
            entries = new List<SecondaryIndexEntry>();
        }

        return entries;
    }

    public static List<int> ExtractDocIds(List<SecondaryIndexEntry> entries)
    {
        HashSet<int> seen = new HashSet<int>(entries.Count);
        List<int> docIds = new List<int>(entries.Count);

        foreach (SecondaryIndexEntry entry in entries)
        {
            if (seen.Add(entry.DocId))
            {
                docIds.Add(entry.DocId);
            }
        }

        return docIds;
    }
}
