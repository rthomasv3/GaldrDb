using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole;

public static class PerformanceTracingTest
{
    /// <summary>
    /// Runs insert workload for profiling with dotnet-trace.
    /// Usage: dotnet-trace collect -- dotnet run -c Release --project GaldrDbConsole -- -tpi
    /// </summary>
    public static void RunInsertProfiling(int iterations = 10000)
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbPerfProfile_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPath = Path.Combine(testDir, "test.galdr");

        try
        {
            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions()))
            {
                // Warmup
                for (int i = 0; i < 500; i++)
                {
                    db.Insert(new BenchmarkPerson
                    {
                        Name = $"Warmup {i}",
                        Age = 25,
                        Email = "warmup@example.com",
                        Address = "Warmup St",
                        Phone = "555-0000"
                    });
                }

                Console.WriteLine($"Starting {iterations} insert iterations for profiling...");
                Console.WriteLine("Attach profiler now or use: dotnet-trace collect --process-id " + Environment.ProcessId);

                Stopwatch sw = Stopwatch.StartNew();

                for (int i = 0; i < iterations; i++)
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

                sw.Stop();
                double avgMicroseconds = (sw.Elapsed.TotalMilliseconds * 1000) / iterations;
                Console.WriteLine($"Completed {iterations} iterations in {sw.Elapsed.TotalMilliseconds:F2} ms");
                Console.WriteLine($"Average: {avgMicroseconds:F2} µs per insert");
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

    /// <summary>
    /// Runs async insert workload for profiling with dotnet-trace.
    /// Usage: dotnet-trace collect -- dotnet run -c Release --project GaldrDbConsole -- -tpia
    /// </summary>
    public static async Task RunInsertAsyncProfiling(int iterations = 10000)
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbPerfProfile_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPath = Path.Combine(testDir, "test.galdr");

        try
        {
            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
            {
                UseWal = true,
                GarbageCollectionThreshold = 500,
            }))
            {
                // Warmup
                for (int i = 0; i < 500; i++)
                {
                    await db.InsertAsync(new BenchmarkPerson
                    {
                        Name = $"Warmup {i}",
                        Age = 25,
                        Email = "warmup@example.com",
                        Address = "Warmup St",
                        Phone = "555-0000"
                    });
                }

                Console.WriteLine($"Starting {iterations} async insert iterations for profiling...");
                Console.WriteLine("Attach profiler now or use: dotnet-trace collect --process-id " + Environment.ProcessId);

                Stopwatch sw = Stopwatch.StartNew();

                for (int i = 0; i < iterations; i++)
                {
                    await db.InsertAsync(new BenchmarkPerson
                    {
                        Name = $"Person {i}",
                        Age = 25,
                        Email = "test@example.com",
                        Address = "456 Oak Ave",
                        Phone = "555-5678"
                    });
                }

                sw.Stop();
                double avgMicroseconds = (sw.Elapsed.TotalMilliseconds * 1000) / iterations;
                Console.WriteLine($"Completed {iterations} async iterations in {sw.Elapsed.TotalMilliseconds:F2} ms");
                Console.WriteLine($"Average: {avgMicroseconds:F2} µs per async insert");
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

    /// <summary>
    /// Runs update workload for profiling with dotnet-trace.
    /// Usage: dotnet-trace collect -- dotnet run -c Release --project GaldrDbConsole -- -tpp
    /// </summary>
    public static void RunUpdateProfiling(int iterations = 10000)
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbPerfProfile_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPath = Path.Combine(testDir, "test.galdr");

        try
        {
            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
            {
                UseWal = true,
                GarbageCollectionThreshold = 500,
            }))
            {
                // Warmup - insert and update many documents
                for (int i = 0; i < 500; i++)
                {
                    int warmupId = db.Insert(new BenchmarkPerson
                    {
                        Name = "To Update",
                        Age = 99,
                        Email = "random@example.com",
                        Address = "Update St",
                        Phone = "555-0000"
                    });
                    db.UpdateById<BenchmarkPerson>(warmupId)
                        .Set(BenchmarkPersonMeta.Age, 31)
                        .Set(BenchmarkPersonMeta.Email, "updated@example.com")
                        .Execute();
                }

                // Insert ONE document (like benchmark IterationSetup)
                int id = db.Insert(new BenchmarkPerson
                {
                    Name = "To Update",
                    Age = 99,
                    Email = "random@example.com",
                    Address = "Update St",
                    Phone = "555-0000"
                });

                Console.WriteLine($"Starting {iterations} update iterations for profiling...");
                Console.WriteLine("(Updating same document repeatedly, like benchmark with invocationCount)");
                Console.WriteLine("Attach profiler now or use: dotnet-trace collect --process-id " + Environment.ProcessId);

                Stopwatch sw = Stopwatch.StartNew();

                // Main loop - update SAME document repeatedly (matches benchmark behavior)
                for (int i = 0; i < iterations; i++)
                {
                    db.UpdateById<BenchmarkPerson>(id)
                        .Set(BenchmarkPersonMeta.Age, 31)
                        .Set(BenchmarkPersonMeta.Email, "updated@example.com")
                        .Execute();
                }

                sw.Stop();
                double avgMicroseconds = (sw.Elapsed.TotalMilliseconds * 1000) / iterations;
                Console.WriteLine($"Completed {iterations} iterations in {sw.Elapsed.TotalMilliseconds:F2} ms");
                Console.WriteLine($"Average: {avgMicroseconds:F2} µs per update");
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

    public static void Run()
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbPerfTest_{Guid.NewGuid()}");
        Directory.CreateDirectory(testDir);
        string dbPath = Path.Combine(testDir, "test.galdr");

        try
        {
            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
            {
                UseWal = true,
                WarmupOnOpen = true
            }))
            {
                int id = db.Insert(new BenchmarkPerson
                {
                    Name = "Test Person",
                    Age = 30,
                    Email = "test@example.com",
                    Address = "123 Main St",
                    Phone = "555-1234"
                });

                // Warmup - run enough iterations to stabilize
                for (int i = 0; i < 200; i++)
                {
                    using (ITransaction tx = db.BeginTransaction())
                    {
                        tx.UpdateById<BenchmarkPerson>(id)
                            .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                            .Execute();
                        tx.Commit();
                    }
                }

                // Trace a single operation with detailed breakdown
                PerfTracer.Enabled = true;
                PerfTracer.Reset();

                using (ITransaction tx = db.BeginTransaction())
                {
                    PerfTracer.Checkpoint("BeginTx");

                    IUpdateBuilder<BenchmarkPerson> builder = tx.UpdateById<BenchmarkPerson>(id);
                    PerfTracer.Checkpoint("UpdateById");

                    builder.Set(BenchmarkPersonMeta.Age, 42);
                    PerfTracer.Checkpoint("Set");

                    builder.Execute();
                    PerfTracer.Checkpoint("Execute");

                    tx.Commit();
                    PerfTracer.Checkpoint("Commit");
                }
                PerfTracer.Checkpoint("Dispose");

                Console.WriteLine("=== UpdateById Performance Breakdown (Single Operation) ===");
                PerfTracer.PrintSummary();

                PerfTracer.Enabled = false;

                // Measure averages over multiple iterations
                MeasureAverages(db, id);
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

    private static void MeasureAverages(GaldrDb db, int id)
    {
        // Additional warmup
        for (int i = 0; i < 50; i++)
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<BenchmarkPerson>(id)
                    .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                    .Execute();
                tx.Commit();
            }
        }

        int iterations = 100;

        // Measure with PerfTracer enabled to get aggregated stats
        PerfTracer.Enabled = true;
        PerfTracer.Reset();

        for (int i = 0; i < iterations; i++)
        {
            using (ITransaction tx = db.BeginTransaction())
            {
                tx.UpdateById<BenchmarkPerson>(id)
                    .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                    .Execute();
                tx.Commit();
            }
        }

        Console.WriteLine($"=== Aggregated Performance ({iterations} iterations) ===");
        PerfTracer.PrintAggregated();

        PerfTracer.Enabled = false;
    }
}
