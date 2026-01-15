using System;
using System.Collections.Generic;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole;

public static class DiagnosticTest
{
    private static int _nextId;

    public static void Run()
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbDiagTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);

        try
        {
            Console.WriteLine("=== Delete Performance Test ===");
            Console.WriteLine();
            RunDeletePerfTest(Path.Combine(testDir, "delete_perf.galdr"));
        }
        finally
        {
            if (Directory.Exists(testDir))
            {
                Directory.Delete(testDir, true);
            }
        }
    }

    private static void RunDeletePerfTest(string dbPath)
    {
        // First test: just inserts to verify no regression
        Console.WriteLine("=== Insert Stress Test ===");

        GaldrDb galdrDb = GaldrDb.Create(dbPath, new GaldrDbOptions
        {
            UseWal = true,
            AutoGarbageCollection = false,
            AutoCheckpoint = false
        });
        _nextId = 1000;

        int[] checkpoints = { 100, 500, 1000, 2000 };
        int totalInserted = 0;

        foreach (int checkpoint in checkpoints)
        {
            int toInsert = checkpoint - totalInserted;
            Console.WriteLine($"Inserting {toInsert} docs to reach {checkpoint}...");

            for (int i = 0; i < toInsert; i++)
            {
                GaldrDb_Insert(galdrDb);
                totalInserted++;
            }

            Console.WriteLine($"  Inserted {totalInserted} total");
        }

        Console.WriteLine();
        Console.WriteLine("=== Delete Scaling Test ===");
        Console.WriteLine("  Total Docs | Avg Delete Time");
        Console.WriteLine("-------------|----------------");

        // Now test deletes at different sizes
        int[] deleteCheckpoints = { 100, 500, 1000 };
        int measurementCount = 50;

        foreach (int checkpoint in deleteCheckpoints)
        {
            // Insert docs to delete
            List<int> insertedIds = new List<int>();
            for (int i = 0; i < measurementCount; i++)
            {
                int id = GaldrDb_Insert(galdrDb);
                insertedIds.Add(id);
            }

            // Measure deletes
            List<double> deleteTimes = new List<double>();
            for (int i = 0; i < measurementCount; i++)
            {
                int idToDelete = insertedIds[i];

                long startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
                galdrDb.Delete<BenchmarkPerson>(idToDelete);
                long endTicks = System.Diagnostics.Stopwatch.GetTimestamp();

                double microseconds = (endTicks - startTicks) * 1_000_000.0 / System.Diagnostics.Stopwatch.Frequency;
                deleteTimes.Add(microseconds);
            }

            deleteTimes.Sort();
            double avgMicroseconds = 0;
            for (int i = 0; i < deleteTimes.Count; i++)
            {
                avgMicroseconds += deleteTimes[i];
            }
            avgMicroseconds /= deleteTimes.Count;

            double medianMicroseconds = deleteTimes[deleteTimes.Count / 2];
            double minMicroseconds = deleteTimes[0];
            double maxMicroseconds = deleteTimes[deleteTimes.Count - 1];

            Console.WriteLine($"  {totalInserted,10} | avg={avgMicroseconds,7:F2} µs, med={medianMicroseconds,7:F2} µs, min={minMicroseconds,6:F2}, max={maxMicroseconds,7:F2}");
        }

        galdrDb.Dispose();

        // Test insert-delete-insert cycle
        Console.WriteLine();
        Console.WriteLine("=== Insert-Delete-Insert Cycle Test ===");

        string dbPath2 = dbPath + ".cycle";
        GaldrDb galdrDb2 = GaldrDb.Create(dbPath2, new GaldrDbOptions
        {
            UseWal = true,
            AutoGarbageCollection = false,
            AutoCheckpoint = false
        });

        Console.WriteLine("Phase 1: Insert 200 docs...");
        List<int> ids = new List<int>();
        for (int i = 0; i < 200; i++)
        {
            ids.Add(galdrDb2.Insert(new BenchmarkPerson
            {
                Name = $"Person {i}",
                Age = 25,
                Email = "test@example.com",
                Address = "456 Oak Ave",
                Phone = "555-5678"
            }));
        }
        Console.WriteLine("  Done.");

        Console.WriteLine("Phase 2: Delete last 100 docs...");
        for (int i = 100; i < 200; i++)
        {
            galdrDb2.Delete<BenchmarkPerson>(ids[i]);
        }
        Console.WriteLine("  Done.");

        Console.WriteLine("Phase 3: Insert 1000 more docs...");
        for (int i = 0; i < 1000; i++)
        {
            galdrDb2.Insert(new BenchmarkPerson
            {
                Name = $"NewPerson {i}",
                Age = 30,
                Email = "new@example.com",
                Address = "789 Pine St",
                Phone = "555-9999"
            });

            if ((i + 1) % 200 == 0)
            {
                Console.WriteLine($"  Inserted {i + 1}...");
            }
        }
        Console.WriteLine("  Done. Test PASSED!");

        galdrDb2.Dispose();
        Console.WriteLine();
    }

    private static int GaldrDb_Insert(GaldrDb galdrDb)
    {
        int id = galdrDb.Insert(new BenchmarkPerson
        {
            Name = $"Person {_nextId++}",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        });

        return id;
    }
}
