using System;
using System.Collections.Generic;
using System.Linq;

namespace GaldrDbConsole.StressTest;

public class StressTestState
{
    private readonly Dictionary<int, ExpectedDocument> _expectedDocuments;
    private readonly object _lock;

    public StressTestState()
    {
        _expectedDocuments = new Dictionary<int, ExpectedDocument>();
        _lock = new object();
    }

    public int DocumentCount
    {
        get
        {
            lock (_lock)
            {
                return _expectedDocuments.Count;
            }
        }
    }

    public void RecordInsert(int docId, byte[] contentHash, int version)
    {
        lock (_lock)
        {
            ExpectedDocument record = new ExpectedDocument
            {
                DocId = docId,
                ContentHash = contentHash,
                Version = version
            };

            _expectedDocuments[docId] = record;
        }
    }

    public void RecordUpdate(int docId, byte[] newContentHash, int newVersion)
    {
        lock (_lock)
        {
            if (_expectedDocuments.TryGetValue(docId, out ExpectedDocument record))
            {
                record.ContentHash = newContentHash;
                record.Version = newVersion;
            }
        }
    }

    public void RecordDelete(int docId)
    {
        lock (_lock)
        {
            _expectedDocuments.Remove(docId);
        }
    }

    public bool DocumentExists(int docId)
    {
        lock (_lock)
        {
            return _expectedDocuments.ContainsKey(docId);
        }
    }

    public byte[] GetExpectedHash(int docId)
    {
        lock (_lock)
        {
            byte[] hash = null;

            if (_expectedDocuments.TryGetValue(docId, out ExpectedDocument record))
            {
                hash = record.ContentHash;
            }

            return hash;
        }
    }

    public int? GetRandomDocumentId(Random rng)
    {
        lock (_lock)
        {
            int? result = null;

            if (_expectedDocuments.Count > 0)
            {
                List<int> ids = _expectedDocuments.Keys.ToList();
                result = ids[rng.Next(ids.Count)];
            }

            return result;
        }
    }

    public List<int> GetAllDocumentIds()
    {
        lock (_lock)
        {
            return _expectedDocuments.Keys.ToList();
        }
    }

    public bool VerifyHash(int docId, byte[] actualHash)
    {
        lock (_lock)
        {
            bool result = false;

            if (_expectedDocuments.TryGetValue(docId, out ExpectedDocument expected))
            {
                if (expected.ContentHash != null && actualHash != null &&
                    expected.ContentHash.Length == actualHash.Length)
                {
                    result = true;
                    for (int i = 0; i < expected.ContentHash.Length; i++)
                    {
                        if (expected.ContentHash[i] != actualHash[i])
                        {
                            result = false;
                            break;
                        }
                    }
                }
            }

            return result;
        }
    }

    public bool VerifyHashAndVersion(int docId, byte[] actualHash, int actualVersion)
    {
        lock (_lock)
        {
            bool result = false;

            if (_expectedDocuments.TryGetValue(docId, out ExpectedDocument expected))
            {
                // If versions differ, a concurrent update happened - not corruption
                if (actualVersion != expected.Version)
                {
                    result = true;
                }
                else if (expected.ContentHash != null && actualHash != null &&
                    expected.ContentHash.Length == actualHash.Length)
                {
                    result = true;
                    for (int i = 0; i < expected.ContentHash.Length; i++)
                    {
                        if (expected.ContentHash[i] != actualHash[i])
                        {
                            result = false;
                            break;
                        }
                    }
                }
            }

            return result;
        }
    }
}

public class ExpectedDocument
{
    public int DocId { get; set; }
    public byte[] ContentHash { get; set; }
    public int Version { get; set; }
}
