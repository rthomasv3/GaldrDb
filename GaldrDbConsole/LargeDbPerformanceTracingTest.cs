using System;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole;

public static class LargeDbPerformanceTracingTest
{
    private static readonly string LargeDbPath = "TEMP/perf_test.db";
    private static readonly string BaselineDbPath = "TEMP/baseline_test.db";
    private static readonly string BaselineWALPath = "TEMP/baseline_test.wal";

    public static void Run()
    {
        Directory.CreateDirectory("TEMP");

        int insertCount = 1000;
        int warmupCount = 100;
        GaldrDbOptions options = new GaldrDbOptions();

        // === BASELINE: Fresh database ===
        Console.WriteLine("=== BASELINE: Fresh Database ===");

        // Delete baseline if it exists
        if (File.Exists(BaselineDbPath))
        {
            File.Delete(BaselineDbPath);
        }
        
        if (File.Exists(BaselineWALPath))
        {
            File.Delete(BaselineWALPath);
        }

        using (GaldrDb db = GaldrDb.Create(BaselineDbPath, options))
        {
            // Warmup
            for (int i = 0; i < warmupCount; i++)
            {
                db.Insert(new BenchmarkPerson
                {
                    Name = $"Warmup {i}",
                    Age = 25,
                    Email = "warmup@example.com",
                    Address = "456 Oak Ave",
                    Phone = "555-5678"
                });
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Trace inserts
            PerfTracer.Enabled = true;
            PerfTracer.Reset();

            for (int i = 0; i < insertCount; i++)
            {
                db.Insert(new BenchmarkPerson
                {
                    Name = $"Person {i}",
                    Age = 25,
                    Email = "test@example.com",
                    Address = "456 Oak Ave",
                    Phone = "555-5678"
                });
            }

            Console.WriteLine($"INSERT Performance ({warmupCount} -> {warmupCount + insertCount} docs)");
            PerfTracer.PrintAggregated();
            PerfTracer.Enabled = false;
        }

        // Delete baseline database
        File.Delete(BaselineDbPath);
        File.Delete(BaselineWALPath);

        // === LARGE DATABASE ===
        Console.WriteLine("\n=== LARGE DATABASE ===");

        if (!File.Exists(LargeDbPath))
        {
            BuildLargeDatabase();
            return;
        }

        using (GaldrDb db = GaldrDb.Open(LargeDbPath, options))
        {
            int docCount = db.Query<BenchmarkPerson>().Count();
            Console.WriteLine($"Current document count: {docCount:N0}");

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Trace inserts
            PerfTracer.Enabled = true;
            PerfTracer.Reset();

            for (int i = 0; i < insertCount; i++)
            {
                db.Insert(new BenchmarkPerson
                {
                    Name = $"Person {i}",
                    Age = 25,
                    Email = "test@example.com",
                    Address = "456 Oak Ave",
                    Phone = "555-5678"
                });
            }

            Console.WriteLine($"INSERT Performance ({docCount:N0} -> {docCount + insertCount:N0} docs)");
            PerfTracer.PrintAggregated();
            PerfTracer.Enabled = false;

            Console.WriteLine($"Final document count: {db.Query<BenchmarkPerson>().Count():N0}");
        }
    }

    private static void BuildLargeDatabase()
    {
        int batches = 500;
        int batchSize = 1000;
        
        using (GaldrDb db = GaldrDb.Create(LargeDbPath, new GaldrDbOptions()))
        {
            for (int batch = 0; batch < batches; ++batch)
            {
                Transaction tx = db.BeginTransaction();

                for (int entry = 0; entry < batchSize; ++entry)
                {
                    tx.Insert(new BenchmarkPerson
                    {
                        Name = $"Person {(batch * batchSize) + entry + 1}",
                        Age = 25,
                        Email = "test@example.com",
                        Address = "456 Oak Ave",
                        Phone = "555-5678"
                    });
                }

                tx.Commit();
            }
        }
    }
}
