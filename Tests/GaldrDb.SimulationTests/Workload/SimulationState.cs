using System;
using System.Collections.Generic;
using System.Linq;
using GaldrDb.SimulationTests.Core;

namespace GaldrDb.SimulationTests.Workload;

public class SimulationState
{
    private readonly Dictionary<string, Dictionary<int, DocumentRecord>> _expectedDocuments;
    private readonly List<CommittedTransaction> _commitHistory;
    private readonly object _lock;

    public SimulationState()
    {
        _expectedDocuments = new Dictionary<string, Dictionary<int, DocumentRecord>>();
        _commitHistory = new List<CommittedTransaction>();
        _lock = new object();
    }

    public void EnsureCollection(string collectionName)
    {
        lock (_lock)
        {
            if (!_expectedDocuments.ContainsKey(collectionName))
            {
                _expectedDocuments[collectionName] = new Dictionary<int, DocumentRecord>();
            }
        }
    }

    public void RecordInsert(string collection, int docId, byte[] contentHash, ulong txId)
    {
        lock (_lock)
        {
            EnsureCollection(collection);

            DocumentRecord record = new DocumentRecord
            {
                DocId = docId,
                ContentHash = contentHash,
                CreatedAtTxId = txId,
                UpdatedAtTxId = null
            };

            _expectedDocuments[collection][docId] = record;
        }
    }

    public void RecordUpdate(string collection, int docId, byte[] newContentHash, ulong txId)
    {
        lock (_lock)
        {
            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                if (docs.TryGetValue(docId, out DocumentRecord record))
                {
                    record.ContentHash = newContentHash;
                    record.UpdatedAtTxId = txId;
                }
            }
        }
    }

    public void RecordDelete(string collection, int docId)
    {
        lock (_lock)
        {
            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                docs.Remove(docId);
            }
        }
    }

    public void RecordCommit(ulong txId, List<OperationRecord> operations)
    {
        lock (_lock)
        {
            CommittedTransaction tx = new CommittedTransaction
            {
                TxId = txId,
                Operations = operations,
                CommittedAt = DateTime.UtcNow
            };
            _commitHistory.Add(tx);
        }
    }

    public int? GetRandomDocumentId(string collection, SimulationRandom rng)
    {
        lock (_lock)
        {
            int? result = null;

            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                if (docs.Count > 0)
                {
                    List<int> ids = docs.Keys.ToList();
                    result = ids[rng.Next(ids.Count)];
                }
            }

            return result;
        }
    }

    public List<int> GetAllDocumentIds(string collection)
    {
        lock (_lock)
        {
            List<int> result = new List<int>();

            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                result.AddRange(docs.Keys);
            }

            return result;
        }
    }

    public int GetExpectedDocumentCount(string collection)
    {
        lock (_lock)
        {
            int result = 0;

            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                result = docs.Count;
            }

            return result;
        }
    }

    public int GetTotalDocumentCount()
    {
        lock (_lock)
        {
            int total = 0;

            foreach (KeyValuePair<string, Dictionary<int, DocumentRecord>> kvp in _expectedDocuments)
            {
                total += kvp.Value.Count;
            }

            return total;
        }
    }

    public DocumentRecord GetExpectedDocument(string collection, int docId)
    {
        lock (_lock)
        {
            DocumentRecord result = null;

            if (_expectedDocuments.TryGetValue(collection, out Dictionary<int, DocumentRecord> docs))
            {
                docs.TryGetValue(docId, out result);
            }

            return result;
        }
    }

    public bool VerifyDocumentHash(string collection, int docId, byte[] actualHash)
    {
        lock (_lock)
        {
            DocumentRecord expected = GetExpectedDocument(collection, docId);

            if (expected == null)
            {
                return false;
            }

            if (expected.ContentHash.Length != actualHash.Length)
            {
                return false;
            }

            for (int i = 0; i < expected.ContentHash.Length; i++)
            {
                if (expected.ContentHash[i] != actualHash[i])
                {
                    return false;
                }
            }

            return true;
        }
    }

    public byte[] GetDocumentHash(string collection, int docId)
    {
        lock (_lock)
        {
            DocumentRecord expected = GetExpectedDocument(collection, docId);
            return expected?.ContentHash;
        }
    }

    public int GetDocumentCount(string collection)
    {
        return GetExpectedDocumentCount(collection);
    }

    public IReadOnlyList<string> GetCollectionNames()
    {
        lock (_lock)
        {
            return _expectedDocuments.Keys.ToList();
        }
    }
}

public class DocumentRecord
{
    public int DocId { get; set; }
    public byte[] ContentHash { get; set; }
    public ulong CreatedAtTxId { get; set; }
    public ulong? UpdatedAtTxId { get; set; }
}

public class CommittedTransaction
{
    public ulong TxId { get; set; }
    public List<OperationRecord> Operations { get; set; }
    public DateTime CommittedAt { get; set; }
}

public class OperationRecord
{
    public string OperationType { get; set; }
    public string Collection { get; set; }
    public int DocId { get; set; }
}
