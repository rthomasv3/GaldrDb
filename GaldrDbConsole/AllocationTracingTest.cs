using System;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole;

public static class AllocationTracingTest
{
    public static void Run()
    {
        string testDir = Path.Combine(Path.GetTempPath(), $"GaldrDbAllocTest_{Guid.NewGuid()}");
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
                    using (Transaction tx = db.BeginTransaction())
                    {
                        tx.UpdateById<BenchmarkPerson>(id)
                            .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                            .Execute();
                        tx.Commit();
                    }
                }

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                // Trace a single operation
                AllocTracer.Enabled = true;
                AllocTracer.Reset();

                using (Transaction tx = db.BeginTransaction())
                {
                    AllocTracer.Checkpoint("BeginTx");

                    UpdateBuilder<BenchmarkPerson> builder = tx.UpdateById<BenchmarkPerson>(id);
                    AllocTracer.Checkpoint("UpdateById");

                    builder.Set(BenchmarkPersonMeta.Age, 42);
                    AllocTracer.Checkpoint("Set");

                    builder.Execute();
                    AllocTracer.Checkpoint("Execute");

                    tx.Commit();
                    // Commit() adds its own checkpoints internally
                }

                Console.WriteLine("=== UpdateById Allocation Breakdown ===");
                AllocTracer.PrintSummary();

                AllocTracer.Enabled = false;

                // Also measure total bytes for comparison
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
        // Additional warmup for this specific test
        for (int i = 0; i < 50; i++)
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.UpdateById<BenchmarkPerson>(id)
                    .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                    .Execute();
                tx.Commit();
            }
            using (Transaction tx = db.BeginTransaction())
            {
                BenchmarkPerson person = tx.GetById<BenchmarkPerson>(id);
                person.Age = 31 + (i % 10);
                tx.Update(person);
                tx.Commit();
            }
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        int iterations = 100;
        long totalUpdateById = 0;
        long totalFullUpdate = 0;

        for (int i = 0; i < iterations; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            using (Transaction tx = db.BeginTransaction())
            {
                tx.UpdateById<BenchmarkPerson>(id)
                    .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                    .Execute();
                tx.Commit();
            }
            long after = GC.GetAllocatedBytesForCurrentThread();
            totalUpdateById += (after - before);
        }

        for (int i = 0; i < iterations; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            using (Transaction tx = db.BeginTransaction())
            {
                BenchmarkPerson person = tx.GetById<BenchmarkPerson>(id);
                person.Age = 31 + (i % 10);
                tx.Update(person);
                tx.Commit();
            }
            long after = GC.GetAllocatedBytesForCurrentThread();
            totalFullUpdate += (after - before);
        }

        Console.WriteLine("=== Average Allocations (100 iterations) ===");
        Console.WriteLine($"  UpdateById:       {totalUpdateById / iterations,5} bytes/op");
        Console.WriteLine($"  GetById+Update:   {totalFullUpdate / iterations,5} bytes/op");
        Console.WriteLine($"  Savings:          {(totalFullUpdate - totalUpdateById) / iterations,5} bytes/op");
    }
}
