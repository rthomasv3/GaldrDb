using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

// Aliases to avoid ambiguity
using SimState = GaldrDb.SimulationTests.Workload.SimulationState;

namespace GaldrDb.SimulationTests.Core;

/// <summary>
/// Types of invariants that can be checked.
/// </summary>
public enum InvariantType
{
    DocumentExists,
    DocumentContent,
    DocumentCount,
    NoUnexpectedDocuments,
    ConsistentReads,
    CollectionExists
}

/// <summary>
/// Represents a violation of a database invariant.
/// </summary>
public class InvariantViolation
{
    public InvariantType Type { get; set; }
    public string CollectionName { get; set; }
    public int? DocumentId { get; set; }
    public string Message { get; set; }
    public DateTime Timestamp { get; set; }

    public InvariantViolation(InvariantType type, string message)
    {
        Type = type;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }

    public InvariantViolation(InvariantType type, string collectionName, string message)
    {
        Type = type;
        CollectionName = collectionName;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }

    public InvariantViolation(InvariantType type, string collectionName, int documentId, string message)
    {
        Type = type;
        CollectionName = collectionName;
        DocumentId = documentId;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }

    public override string ToString()
    {
        string docInfo = DocumentId.HasValue ? $" (Doc {DocumentId})" : "";
        string colInfo = !string.IsNullOrEmpty(CollectionName) ? $" [{CollectionName}]" : "";
        return $"[{Type}]{colInfo}{docInfo}: {Message}";
    }
}

/// <summary>
/// Result of an invariant check operation.
/// </summary>
public class InvariantCheckResult
{
    public bool AllPassed { get; set; }
    public int ChecksPerformed { get; set; }
    public int ViolationsFound { get; set; }
    public List<InvariantViolation> Violations { get; set; }
    public TimeSpan Duration { get; set; }

    public InvariantCheckResult()
    {
        Violations = new List<InvariantViolation>();
        AllPassed = true;
    }

    public void AddViolation(InvariantViolation violation)
    {
        Violations.Add(violation);
        ViolationsFound++;
        AllPassed = false;
    }
}

/// <summary>
/// Verifies database invariants during simulation testing.
/// Invariants are properties that should always hold true.
/// </summary>
public class InvariantChecker
{
    private readonly SimulationStats _stats;

    public InvariantChecker(SimulationStats stats)
    {
        _stats = stats;
    }

    /// <summary>
    /// Performs a full invariant check against the database and expected state.
    /// </summary>
    public InvariantCheckResult CheckAll(GaldrDbEngine.GaldrDb db, SimulationState expectedState)
    {
        DateTime startTime = DateTime.UtcNow;
        InvariantCheckResult result = new InvariantCheckResult();

        // Check each collection
        foreach (string collectionName in expectedState.GetCollectionNames())
        {
            CheckCollectionInvariants(db, expectedState, collectionName, result);
        }

        // Check for consistent reads
        CheckConsistentReads(db, expectedState, result);

        result.Duration = DateTime.UtcNow - startTime;
        _stats.InvariantChecks++;

        return result;
    }

    /// <summary>
    /// Checks invariants for a specific collection.
    /// </summary>
    public void CheckCollectionInvariants(
        GaldrDbEngine.GaldrDb db,
        SimulationState expectedState,
        string collectionName,
        InvariantCheckResult result)
    {
        List<int> expectedDocIds = expectedState.GetAllDocumentIds(collectionName);

        using (Transaction tx = db.BeginReadOnlyTransaction())
        {
            // Check each expected document exists and has correct content
            foreach (int docId in expectedDocIds)
            {
                result.ChecksPerformed++;

                TestDocument doc = tx.GetById<TestDocument>(docId);

                if (doc == null)
                {
                    result.AddViolation(new InvariantViolation(
                        InvariantType.DocumentExists,
                        collectionName,
                        docId,
                        $"Document {docId} expected to exist but was not found"));
                    continue;
                }

                // Verify content hash
                byte[] actualHash = doc.ComputeHash();
                if (!expectedState.VerifyDocumentHash(collectionName, docId, actualHash))
                {
                    byte[] expectedHash = expectedState.GetDocumentHash(collectionName, docId);
                    result.AddViolation(new InvariantViolation(
                        InvariantType.DocumentContent,
                        collectionName,
                        docId,
                        $"Document {docId} content mismatch. Expected hash: {BitConverter.ToString(expectedHash ?? Array.Empty<byte>())}, Actual: {BitConverter.ToString(actualHash)}"));
                }
            }

            // Check document count matches
            result.ChecksPerformed++;
            int expectedCount = expectedState.GetDocumentCount(collectionName);
            int actualCount = CountDocumentsInCollection(tx, expectedDocIds);

            if (actualCount != expectedCount)
            {
                result.AddViolation(new InvariantViolation(
                    InvariantType.DocumentCount,
                    collectionName,
                    $"Document count mismatch. Expected: {expectedCount}, Actual: {actualCount}"));
            }
        }
    }

    /// <summary>
    /// Checks that multiple reads return consistent results.
    /// </summary>
    public void CheckConsistentReads(
        GaldrDbEngine.GaldrDb db,
        SimulationState expectedState,
        InvariantCheckResult result)
    {
        foreach (string collectionName in expectedState.GetCollectionNames())
        {
            List<int> docIds = expectedState.GetAllDocumentIds(collectionName);

            if (docIds.Count == 0)
            {
                continue;
            }

            // Pick a document to check consistency
            int testDocId = docIds[0];

            result.ChecksPerformed++;

            // Read the same document twice in separate transactions
            byte[] hash1 = null;
            byte[] hash2 = null;

            using (Transaction tx1 = db.BeginReadOnlyTransaction())
            {
                TestDocument doc1 = tx1.GetById<TestDocument>(testDocId);
                if (doc1 != null)
                {
                    hash1 = doc1.ComputeHash();
                }
            }

            using (Transaction tx2 = db.BeginReadOnlyTransaction())
            {
                TestDocument doc2 = tx2.GetById<TestDocument>(testDocId);
                if (doc2 != null)
                {
                    hash2 = doc2.ComputeHash();
                }
            }

            // Both reads should return the same result
            if (hash1 == null && hash2 == null)
            {
                // Both null is consistent (document doesn't exist)
                continue;
            }

            if (hash1 == null || hash2 == null)
            {
                result.AddViolation(new InvariantViolation(
                    InvariantType.ConsistentReads,
                    collectionName,
                    testDocId,
                    $"Inconsistent reads: first read {(hash1 == null ? "not found" : "found")}, second read {(hash2 == null ? "not found" : "found")}"));
                continue;
            }

            bool hashesMatch = true;
            if (hash1.Length != hash2.Length)
            {
                hashesMatch = false;
            }
            else
            {
                for (int i = 0; i < hash1.Length; i++)
                {
                    if (hash1[i] != hash2[i])
                    {
                        hashesMatch = false;
                        break;
                    }
                }
            }

            if (!hashesMatch)
            {
                result.AddViolation(new InvariantViolation(
                    InvariantType.ConsistentReads,
                    collectionName,
                    testDocId,
                    $"Inconsistent reads: content differs between two consecutive reads"));
            }
        }
    }

    /// <summary>
    /// Quick check that verifies basic invariants without full verification.
    /// Useful for frequent checks during simulation.
    /// </summary>
    public bool QuickCheck(GaldrDbEngine.GaldrDb db, SimulationState expectedState)
    {
        _stats.InvariantChecks++;

        // Just verify document counts match
        foreach (string collectionName in expectedState.GetCollectionNames())
        {
            int expectedCount = expectedState.GetDocumentCount(collectionName);
            List<int> docIds = expectedState.GetAllDocumentIds(collectionName);

            using (Transaction tx = db.BeginReadOnlyTransaction())
            {
                int foundCount = 0;
                foreach (int docId in docIds)
                {
                    TestDocument doc = tx.GetById<TestDocument>(docId);
                    if (doc != null)
                    {
                        foundCount++;
                    }
                }

                if (foundCount != expectedCount)
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Verifies a single document matches expected state.
    /// </summary>
    public bool VerifyDocument(GaldrDbEngine.GaldrDb db, SimulationState expectedState, string collectionName, int docId)
    {
        _stats.InvariantChecks++;

        using (Transaction tx = db.BeginReadOnlyTransaction())
        {
            TestDocument doc = tx.GetById<TestDocument>(docId);

            if (doc == null)
            {
                return false;
            }

            byte[] actualHash = doc.ComputeHash();
            return expectedState.VerifyDocumentHash(collectionName, docId, actualHash);
        }
    }

    private int CountDocumentsInCollection(Transaction tx, List<int> docIds)
    {
        int count = 0;

        foreach (int docId in docIds)
        {
            TestDocument doc = tx.GetById<TestDocument>(docId);
            if (doc != null)
            {
                count++;
            }
        }

        return count;
    }
}
