using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload;
using GaldrDbEngine;
using GaldrDbEngine.MVCC;
using GaldrDbEngine.Query;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.WAL;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// Alias to avoid ambiguity
using IReadOnlyDictionaryIntBytes = System.Collections.Generic.IReadOnlyDictionary<int, byte[]>;

namespace GaldrDb.SimulationTests.Tests;

[TestClass]
public class CrashDebugTests
{
    [TestMethod]
    public void Debug_TraceCreateAndRecover()
    {
        SimulationStats stats = new SimulationStats();
        SimulationPageIO pageIO = new SimulationPageIO(8192, stats);
        SimulationWalStream walStream = new SimulationWalStream(stats);
        SimulationWalStreamIO walStreamIO = new SimulationWalStreamIO(walStream);
        SimulationRandom rng = new SimulationRandom(42);

        Console.WriteLine("=== PHASE 1: Create database ===");

        GaldrDbOptions options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        GaldrDbEngine.GaldrDb db = GaldrDbEngine.GaldrDb.Create("debug.db", options);

        Console.WriteLine($"After Create - Persisted pages: {pageIO.PersistedPageCount}, Unflushed: {pageIO.UnflushedPageCount}");
        Console.WriteLine($"After Create - WAL length: {walStream.Length}, Persisted length: {walStream.PersistedLength}");

        Console.WriteLine("\n=== PHASE 2: Insert document ===");

        int insertedId;
        using (ITransaction tx = db.BeginTransaction())
        {
            TestDocument doc = TestDocument.Generate(rng, 200);
            insertedId = tx.Insert(doc);
            Console.WriteLine($"Inserted doc ID: {insertedId}");
            tx.Commit();
            Console.WriteLine("Committed transaction");
        }

        Console.WriteLine($"After Insert - Persisted pages: {pageIO.PersistedPageCount}, Unflushed: {pageIO.UnflushedPageCount}");
        Console.WriteLine($"After Insert - WAL length: {walStream.Length}, Persisted length: {walStream.PersistedLength}");

        Console.WriteLine("\n=== PHASE 3: Dispose database ===");
        db.Dispose();

        Console.WriteLine($"After Dispose - Persisted pages: {pageIO.PersistedPageCount}, Unflushed: {pageIO.UnflushedPageCount}");
        Console.WriteLine($"After Dispose - WAL length: {walStream.Length}, Persisted length: {walStream.PersistedLength}");

        Console.WriteLine("\n=== PHASE 4: Simulate crash ===");
        pageIO.SimulateCrash();
        walStream.SimulateCrash();

        Console.WriteLine($"After Crash - Persisted pages: {pageIO.PersistedPageCount}, Unflushed: {pageIO.UnflushedPageCount}");
        Console.WriteLine($"After Crash - WAL length: {walStream.Length}, Position: {walStream.Position}");

        Console.WriteLine("\n=== PHASE 4.5: Inspect WAL before recovery ===");
        // Create a temporary WAL to read frames
        WriteAheadLog tempWal = new WriteAheadLog("temp.wal", 8192);
        tempWal._testStreamIO = walStreamIO;
        tempWal._testSaltGenerator = () => rng.NextUInt();
        tempWal.Open();

        HashSet<ulong> committedTxIds = tempWal.GetCommittedTransactions();
        Dictionary<ulong, List<WalFrame>> framesByTx = tempWal.GetFramesByTransaction();
        Console.WriteLine($"Committed transactions: {committedTxIds.Count}");
        foreach (ulong txId in committedTxIds)
        {
            Console.WriteLine($"  TxId {txId}");
            if (framesByTx.TryGetValue(txId, out List<WalFrame> frames))
            {
                Console.WriteLine($"    Frames: {frames.Count}");
                foreach (WalFrame frame in frames)
                {
                    Console.WriteLine($"      PageId: {frame.PageId}, DataLen: {frame.Data.Length}, IsCommit: {frame.IsCommit}");
                }
            }
        }
        // Reset stream position for actual recovery
        walStream.Position = 0;

        Console.WriteLine("\n=== PHASE 5: Recover (Open) ===");
        Console.WriteLine($"WAL stream position before Open: {walStream.Position}");
        Console.WriteLine($"Before Open - Persisted pages: {pageIO.PersistedPageCount}");
        Console.WriteLine($"Persisted page IDs before Open: [{string.Join(", ", pageIO.GetPersistedPages().Keys)}]");

        // Hook to trace writes during Open
        pageIO.OnWritePage = (pageId, data) =>
        {
            bool allZeros = true;
            for (int i = 0; i < Math.Min(64, data.Length) && allZeros; i++)
            {
                if (data[i] != 0) allZeros = false;
            }
            Console.WriteLine($"  WRITE: Page {pageId}, first bytes: {BitConverter.ToString(data, 0, 16)}, allZeros={allZeros}");
        };

        options = new GaldrDbOptions
        {
            PageSize = 8192,
            UseWal = true,
            CustomPageIO = pageIO,
            CustomWalStreamIO = walStreamIO,
            CustomWalSaltGenerator = () => rng.NextUInt()
        };

        try
        {
            GaldrDbEngine.GaldrDb db2 = GaldrDbEngine.GaldrDb.Open("debug.db", options);
            pageIO.OnWritePage = null; // Disable hook after Open
            Console.WriteLine($"Persisted page IDs after Open: [{string.Join(", ", pageIO.GetPersistedPages().Keys)}]");
            Console.WriteLine($"Unflushed page IDs after Open: [{string.Join(", ", pageIO.GetUnflushedPages().Keys)}]");
            Console.WriteLine("Open succeeded");
            Console.WriteLine($"After Open - Persisted pages: {pageIO.PersistedPageCount}, Unflushed: {pageIO.UnflushedPageCount}");
            Console.WriteLine($"After Open - WAL length: {walStream.Length}");

            Console.WriteLine("\n=== PHASE 5.5: Inspect CollectionsMetadata after recovery ===");
            GaldrDbEngine.Storage.CollectionsMetadata metadata = db2.CollectionsMetadata;
            List<GaldrDbEngine.Storage.CollectionEntry> metadataCollections = metadata.GetAllCollections();
            Console.WriteLine($"CollectionsMetadata has {metadataCollections.Count} collections:");
            foreach (GaldrDbEngine.Storage.CollectionEntry entry in metadataCollections)
            {
                Console.WriteLine($"  Collection '{entry.Name}': RootPage={entry.RootPage}, NextId={entry.NextId}");

                // Read page 4 raw bytes to see what's there
                Console.WriteLine($"  Inspecting raw page {entry.RootPage}:");

                // Check persisted directly
                IReadOnlyDictionary<int, byte[]> persistedPages = pageIO.GetPersistedPages();
                IReadOnlyDictionary<int, byte[]> unflushedPages = pageIO.GetUnflushedPages();

                if (persistedPages.TryGetValue(entry.RootPage, out byte[] persistedPage4))
                {
                    Console.WriteLine($"    Persisted page {entry.RootPage} first 64 bytes: {BitConverter.ToString(persistedPage4, 0, 64)}");
                }
                else
                {
                    Console.WriteLine($"    Page {entry.RootPage} NOT in persisted!");
                }

                if (unflushedPages.TryGetValue(entry.RootPage, out byte[] unflushedPage4))
                {
                    Console.WriteLine($"    Unflushed page {entry.RootPage} first 64 bytes: {BitConverter.ToString(unflushedPage4, 0, 64)}");
                }
                else
                {
                    Console.WriteLine($"    Page {entry.RootPage} NOT in unflushed");
                }

                byte[] pageBuffer = new byte[8192];
                pageIO.ReadPage(entry.RootPage, pageBuffer);
                Console.WriteLine($"    ReadPage result first 64 bytes: {BitConverter.ToString(pageBuffer, 0, 64)}");

                // Check if page has valid BTree header
                int keyCount = BitConverter.ToInt32(pageBuffer, 0);
                int nodeType = BitConverter.ToInt32(pageBuffer, 4);
                Console.WriteLine($"    KeyCount={keyCount}, NodeType={nodeType}");
            }

            Console.WriteLine("\n=== PHASE 5.6: Inspect VersionIndex after recovery ===");
            VersionIndex versionIndex = db2.VersionIndex;
            List<string> collectionNames = versionIndex.GetCollectionNames();
            Console.WriteLine($"VersionIndex has {collectionNames.Count} collections:");
            foreach (string colName in collectionNames)
            {
                int docCount = versionIndex.GetDocumentCount(colName);
                List<int> docIds = versionIndex.GetDocumentIds(colName);
                Console.WriteLine($"  Collection '{colName}': {docCount} documents, IDs: [{string.Join(", ", docIds)}]");
            }

            Console.WriteLine("\n=== PHASE 5.6: Check TestDocument collection name ===");
            GaldrTypeInfo<TestDocument> typeInfo = GaldrTypeRegistry.Get<TestDocument>();
            string expectedCollectionName = typeInfo.CollectionName;
            Console.WriteLine($"TestDocument collection name from GaldrTypeRegistry: '{expectedCollectionName}'");

            Console.WriteLine("\n=== PHASE 6: Query document ===");
            using (ITransaction tx = db2.BeginReadOnlyTransaction())
            {
                Console.WriteLine($"Transaction SnapshotTxId: {tx.SnapshotTxId}");

                // Try to get the visible version directly
                DocumentVersion directVersion = versionIndex.GetVisibleVersion(expectedCollectionName, insertedId, tx.SnapshotCSN);
                Console.WriteLine($"Direct GetVisibleVersion for '{expectedCollectionName}' ID {insertedId}: {(directVersion != null ? "FOUND" : "NULL")}");

                if (directVersion != null)
                {
                    Console.WriteLine($"  Version CreatedBy: {directVersion.CreatedBy}, DeletedCSN: {directVersion.DeletedCSN}");
                    Console.WriteLine($"  Version Location: PageId={directVersion.Location.PageId}, SlotIndex={directVersion.Location.SlotIndex}");
                }

                TestDocument recovered = tx.GetById<TestDocument>(insertedId);
                if (recovered == null)
                {
                    Console.WriteLine($"Document {insertedId} NOT FOUND");
                }
                else
                {
                    Console.WriteLine($"Document {insertedId} found: Name={recovered.Name}");
                }
            }

            db2.Dispose();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Open failed: {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            throw;
        }
    }
}
