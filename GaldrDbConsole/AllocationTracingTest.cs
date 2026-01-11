using System;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

namespace GaldrDbConsole;

public static class AllocationTracingTest
{
    public static void Run()
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbAllocTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPathNoIndex = Path.Combine(testDir, "test_noindex.galdr");
        string dbPathWithIndex = Path.Combine(testDir, "test_withindex.galdr");

        try
        {
            Console.WriteLine("=== Secondary Index Allocation Test ===");
            Console.WriteLine("Comparing allocations: with vs without secondary index");
            Console.WriteLine();

            // Test 1: Without secondary index
            Console.WriteLine("--- Test 1: Without Secondary Index ---");
            long[] noIndexAllocs = RunInsertTest<BenchmarkPersonNoIndex>(dbPathNoIndex, "NoIndex");

            // Test 2: With secondary index
            Console.WriteLine();
            Console.WriteLine("--- Test 2: With Secondary Index (Name field) ---");
            long[] withIndexAllocs = RunInsertTest<BenchmarkPerson>(dbPathWithIndex, "WithIndex");

            // Summary comparison
            Console.WriteLine();
            Console.WriteLine("=== Summary Comparison ===");
            Console.WriteLine();
            Console.WriteLine("Batch    | No Index    | With Index  | Difference  | Overhead");
            Console.WriteLine("---------|-------------|-------------|-------------|----------");

            for (int i = 0; i < 10; i++)
            {
                long diff = withIndexAllocs[i] - noIndexAllocs[i];
                double overhead = noIndexAllocs[i] > 0 ? (double)diff / noIndexAllocs[i] * 100 : 0;
                Console.WriteLine($"Batch {i + 1,2} | {noIndexAllocs[i],8} B | {withIndexAllocs[i],8} B | {diff,+8} B | {overhead,6:F1}%");
            }

            Console.WriteLine();
            long avgNoIndex = 0, avgWithIndex = 0;
            for (int i = 0; i < 10; i++)
            {
                avgNoIndex += noIndexAllocs[i];
                avgWithIndex += withIndexAllocs[i];
            }
            avgNoIndex /= 10;
            avgWithIndex /= 10;
            long avgDiff = avgWithIndex - avgNoIndex;
            double avgOverhead = avgNoIndex > 0 ? (double)avgDiff / avgNoIndex * 100 : 0;

            Console.WriteLine($"Average  | {avgNoIndex,8} B | {avgWithIndex,8} B | {avgDiff,+8} B | {avgOverhead,6:F1}%");
            Console.WriteLine();
            Console.WriteLine($"Secondary index adds ~{avgDiff:N0} bytes ({avgOverhead:F1}%) per insert on average");
        }
        finally
        {
            if (Directory.Exists(testDir))
            {
                Directory.Delete(testDir, true);
            }
        }
    }

    private static long[] RunInsertTest<T>(string dbPath, string label) where T : new()
    {
        long[] batchAllocs = new long[10];

        using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
        {
            UseWal = true,
            WarmupOnOpen = true,
            AutoGarbageCollection = true
        }))
        {
            int batchIndex = 0;
            long batchTotal = 0;

            for (int i = 0; i < 100000; i++)
            {
                T person = CreatePerson<T>(i);

                long before = GC.GetAllocatedBytesForCurrentThread();

                Transaction tx = db.BeginTransaction();
                tx.Insert(person);
                tx.Commit();
                tx.Dispose();

                long after = GC.GetAllocatedBytesForCurrentThread();
                batchTotal += (after - before);

                if ((i + 1) % 10000 == 0 && batchIndex < 10)
                {
                    batchAllocs[batchIndex] = batchTotal / 10000;
                    Console.WriteLine($"  Batch {batchIndex + 1}: {batchAllocs[batchIndex]} bytes/insert avg");
                    batchTotal = 0;
                    batchIndex++;
                }
            }
        }

        return batchAllocs;
    }

    private static T CreatePerson<T>(int index) where T : new()
    {
        T person = new T();

        // Use reflection-free approach by checking type
        if (person is BenchmarkPersonNoIndex noIndex)
        {
            noIndex.Name = $"Person {index}";
            noIndex.Age = 25 + (index % 50);
            noIndex.Email = $"person{index}@example.com";
            noIndex.Address = "123 Main St";
            noIndex.Phone = "555-1234";
        }
        else if (person is BenchmarkPerson withIndex)
        {
            withIndex.Name = $"Person {index}";
            withIndex.Age = 25 + (index % 50);
            withIndex.Email = $"person{index}@example.com";
            withIndex.Address = "123 Main St";
            withIndex.Phone = "555-1234";
        }

        return person;
    }
}
