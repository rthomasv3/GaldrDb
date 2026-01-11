using System;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole;

public static class InsertTest
{
    public static void Run()
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbInsertTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPath = Path.Combine(testDir, "test.galdr");

        try
        {
            Console.WriteLine("=== Insert Test with JsonWriterPool ===");
            Console.WriteLine();

            JsonWriterPool.ResetStats();

            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
            {
                UseWal = true,
                WarmupOnOpen = true,
                JsonWriterPoolWarmupCount = 4
            }))
            {
                Console.WriteLine("Warmup complete. Performing inserts...");
                Console.WriteLine($"Pool stats after warmup: Hits={JsonWriterPool.Stats.Hits}, Misses={JsonWriterPool.Stats.Misses}");
                Console.WriteLine();

                long totalAlloc = 0;
                for (int i = 0; i < 1000; i++)
                {
                    long before = GC.GetAllocatedBytesForCurrentThread();
                    // Include object creation in measurement to match BenchmarkDotNet
                    BenchmarkPerson newPerson = new BenchmarkPerson
                    {
                        Name = $"Test Person {i}",
                        Age = 25 + i,
                        Email = $"test{i}@example.com",
                        Address = "456 Oak Ave",
                        Phone = "555-5678"
                    };
                    db.Insert(newPerson);
                    long after = GC.GetAllocatedBytesForCurrentThread();
                    long alloc = after - before;
                    totalAlloc += alloc;
                    if (i < 10 || i % 100 == 0)
                    {
                        Console.WriteLine($"Insert #{i + 1}: +{alloc} bytes");
                    }
                }
                Console.WriteLine();
                Console.WriteLine($"Total for 1000 inserts: {totalAlloc} bytes, avg: {totalAlloc / 1000} bytes/insert");
                Console.WriteLine($"JsonWriterPool: Hits={JsonWriterPool.Stats.Hits}, Misses={JsonWriterPool.Stats.Misses}");
                Console.WriteLine($"ListPool<int>: Hits={ListPool<int>.Stats.Hits}, Misses={ListPool<int>.Stats.Misses}, CapMismatch={ListPool<int>.Stats.CapacityMismatches}");
                Console.WriteLine($"ListPool<DocumentLocation>: Hits={ListPool<DocumentLocation>.Stats.Hits}, Misses={ListPool<DocumentLocation>.Stats.Misses}, CapMismatch={ListPool<DocumentLocation>.Stats.CapacityMismatches}");
            }

            Console.WriteLine("=== Test without warmup ===");
            Console.WriteLine();

            string dbPath2 = Path.Combine(testDir, "test2.galdr");
            using (GaldrDb db = GaldrDb.Create(dbPath2, new GaldrDbOptions
            {
                UseWal = true,
                WarmupOnOpen = false
            }))
            {
                Console.WriteLine("No warmup. Performing inserts...");
                Console.WriteLine();

                for (int i = 0; i < 3; i++)
                {
                    Console.WriteLine($"--- Insert #{i + 1} ---");
                    BenchmarkPerson newPerson = new BenchmarkPerson
                    {
                        Name = $"Test Person {i}",
                        Age = 25 + i,
                        Email = $"test{i}@example.com",
                        Address = "456 Oak Ave",
                        Phone = "555-5678"
                    };
                    long before = GC.GetAllocatedBytesForCurrentThread();
                    db.Insert(newPerson);
                    long after = GC.GetAllocatedBytesForCurrentThread();
                    Console.WriteLine($"Total insert alloc: +{after - before} bytes");
                    Console.WriteLine();
                }
            }
        }
        finally
        {
            if (Directory.Exists(testDir))
            {
                Directory.Delete(testDir, true);
            }
        }
    }
}
