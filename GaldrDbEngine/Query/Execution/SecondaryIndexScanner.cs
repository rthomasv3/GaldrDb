using System.Collections.Generic;
using GaldrDbEngine.Query.Planning;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Transactions;

namespace GaldrDbEngine.Query.Execution;

internal sealed class SecondaryIndexScanner
{
    private readonly GaldrDb _db;
    private readonly string _collectionName;
    private readonly TransactionContext _context;

    public SecondaryIndexScanner(GaldrDb db, string collectionName, TransactionContext context)
    {
        _db = db;
        _collectionName = collectionName;
        _context = context;
    }

    public List<SecondaryIndexEntry> GetEntries(QueryPlan plan)
    {
        return GetEntriesCore(plan.IndexDefinition, plan.IndexFilter);
    }

    public List<SecondaryIndexEntry> GetEntries(SecondaryIndexSpec indexSpec)
    {
        List<SecondaryIndexEntry> result;

        if (indexSpec.IsCompoundScan)
        {
            result = GetCompoundEntries(indexSpec);
        }
        else
        {
            result = GetEntriesCore(indexSpec.IndexDefinition, indexSpec.IndexFilter);
        }

        return result;
    }

    private List<SecondaryIndexEntry> GetCompoundEntries(SecondaryIndexSpec indexSpec)
    {
        List<SecondaryIndexEntry> entries;
        IndexDefinition indexDef = indexSpec.IndexDefinition;
        byte[] startKey = indexSpec.CompoundStartKey;
        byte[] endKey = indexSpec.CompoundEndKey;
        SecondaryIndexOperation operation = indexSpec.Operation;

        if (operation == SecondaryIndexOperation.ExactMatch)
        {
            entries = _db.SearchSecondaryIndexExact(_collectionName, indexDef, startKey, _context);
        }
        else if (operation == SecondaryIndexOperation.PrefixMatch)
        {
            entries = _db.SearchSecondaryIndex(_collectionName, indexDef, startKey, _context);
        }
        else if (operation == SecondaryIndexOperation.RangeScan)
        {
            bool includeStart = true;
            bool includeEnd = true;
            if (indexSpec.MatchedFilters != null && indexSpec.MatchedFilters.Count > 0)
            {
                IFieldFilter lastFilter = indexSpec.MatchedFilters[indexSpec.MatchedFilters.Count - 1];
                includeEnd = lastFilter.Operation != FieldOp.LessThan;
            }
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, startKey, endKey, includeStart, includeEnd, _context);
        }
        else if (operation == SecondaryIndexOperation.PrefixRangeScan)
        {
            byte[] prefixKey = indexSpec.CompoundPrefixKey;
            bool includeStart = true;
            if (indexSpec.MatchedFilters != null && indexSpec.MatchedFilters.Count > 0)
            {
                IFieldFilter lastFilter = indexSpec.MatchedFilters[indexSpec.MatchedFilters.Count - 1];
                includeStart = lastFilter.Operation != FieldOp.GreaterThan;
            }
            entries = _db.SearchSecondaryIndexPrefixRange(_collectionName, indexDef, startKey, prefixKey, includeStart, _context);
        }
        else
        {
            entries = new List<SecondaryIndexEntry>();
        }

        return entries;
    }

    private List<SecondaryIndexEntry> GetEntriesCore(IndexDefinition indexDef, IFieldFilter filter)
    {
        List<SecondaryIndexEntry> entries;
        byte[] keyBytes = filter.GetIndexKeyBytes();

        if (filter.Operation == FieldOp.Equals)
        {
            entries = _db.SearchSecondaryIndexExact(_collectionName, indexDef, keyBytes, _context);
        }
        else if (filter.Operation == FieldOp.StartsWith)
        {
            entries = _db.SearchSecondaryIndex(_collectionName, indexDef, keyBytes, _context);
        }
        else if (filter.Operation == FieldOp.In)
        {
            entries = new List<SecondaryIndexEntry>();
            HashSet<int> seenDocIds = new HashSet<int>();
            foreach (byte[] valueKeyBytes in filter.GetAllIndexKeyBytes())
            {
                List<SecondaryIndexEntry> valueEntries = _db.SearchSecondaryIndexExact(_collectionName, indexDef, valueKeyBytes, _context);
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
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, keyBytes, endKeyBytes, true, true, _context);
        }
        else if (filter.Operation == FieldOp.GreaterThan)
        {
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, keyBytes, null, false, true, _context);
        }
        else if (filter.Operation == FieldOp.GreaterThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, keyBytes, null, true, true, _context);
        }
        else if (filter.Operation == FieldOp.LessThan)
        {
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, false, _context);
        }
        else if (filter.Operation == FieldOp.LessThanOrEqual)
        {
            entries = _db.SearchSecondaryIndexRange(_collectionName, indexDef, IndexKeyEncoder.MinimumNonNullKey, keyBytes, true, true, _context);
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
