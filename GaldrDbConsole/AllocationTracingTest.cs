using System;
using System.IO;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Transactions;

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
            Console.WriteLine("=== UpdateById Allocation Tracing ===");
            Console.WriteLine();

            using (GaldrDb db = GaldrDb.Create(dbPath, new GaldrDbOptions
            {
                UseWal = true,
                WarmupOnOpen = true
            }))
            {
                // Insert a document to update
                int id = db.Insert(new BenchmarkPerson
                {
                    Name = "Test Person",
                    Age = 30,
                    Email = "test@example.com",
                    Address = "123 Main St",
                    Phone = "555-1234"
                });

                // Warm up
                for (int i = 0; i < 100; i++)
                {
                    db.UpdateById<BenchmarkPerson>(id)
                        .Set(BenchmarkPersonMeta.Age, 31)
                        .Execute();
                }

                // Force GC to get clean baseline
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                Console.WriteLine("--- UpdateById (single field) ---");
                long[] updateByIdAllocs = new long[10];
                for (int batch = 0; batch < 10; batch++)
                {
                    long batchTotal = 0;
                    for (int i = 0; i < 1000; i++)
                    {
                        long before = GC.GetAllocatedBytesForCurrentThread();

                        db.UpdateById<BenchmarkPerson>(id)
                            .Set(BenchmarkPersonMeta.Age, 31 + (i % 10))
                            .Execute();

                        long after = GC.GetAllocatedBytesForCurrentThread();
                        batchTotal += (after - before);
                    }
                    updateByIdAllocs[batch] = batchTotal / 1000;
                    Console.WriteLine($"  Batch {batch + 1}: {updateByIdAllocs[batch]} bytes/update avg");
                }

                Console.WriteLine();
                Console.WriteLine("--- GetById + Update (single field) ---");
                long[] fullUpdateAllocs = new long[10];
                for (int batch = 0; batch < 10; batch++)
                {
                    long batchTotal = 0;
                    for (int i = 0; i < 1000; i++)
                    {
                        long before = GC.GetAllocatedBytesForCurrentThread();

                        BenchmarkPerson person = db.GetById<BenchmarkPerson>(id);
                        person.Age = 31 + (i % 10);
                        db.Update(person);

                        long after = GC.GetAllocatedBytesForCurrentThread();
                        batchTotal += (after - before);
                    }
                    fullUpdateAllocs[batch] = batchTotal / 1000;
                    Console.WriteLine($"  Batch {batch + 1}: {fullUpdateAllocs[batch]} bytes/update avg");
                }

                // Summary
                Console.WriteLine();
                Console.WriteLine("=== Summary ===");
                long avgUpdateById = 0, avgFullUpdate = 0;
                for (int i = 0; i < 10; i++)
                {
                    avgUpdateById += updateByIdAllocs[i];
                    avgFullUpdate += fullUpdateAllocs[i];
                }
                avgUpdateById /= 10;
                avgFullUpdate /= 10;

                Console.WriteLine($"UpdateById avg:     {avgUpdateById,6} bytes/update");
                Console.WriteLine($"GetById+Update avg: {avgFullUpdate,6} bytes/update");
                Console.WriteLine($"Difference:         {avgFullUpdate - avgUpdateById,6} bytes ({(avgFullUpdate > 0 ? (double)(avgFullUpdate - avgUpdateById) / avgFullUpdate * 100 : 0):F1}% savings)");

                // Detailed breakdown using transaction
                Console.WriteLine();
                Console.WriteLine("=== Detailed Breakdown (with transaction) ===");
                TraceDetailedAllocations(db, id);
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

    private static void TraceDetailedAllocations(GaldrDb db, int id)
    {
        // Warm up
        for (int i = 0; i < 100; i++)
        {
            using (Transaction tx = db.BeginTransaction())
            {
                tx.UpdateById<BenchmarkPerson>(id)
                    .Set(BenchmarkPersonMeta.Age, 31)
                    .Execute();
                tx.Commit();
            }
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Measure individual steps (average of 100 iterations)
        long beginTxTotal = 0;
        long updateByIdTotal = 0;
        long setTotal = 0;
        long executeTotal = 0;
        long commitTotal = 0;
        long disposeTotal = 0;

        int iterations = 100;

        for (int i = 0; i < iterations; i++)
        {
            long before, after;

            before = GC.GetAllocatedBytesForCurrentThread();
            Transaction tx = db.BeginTransaction();
            after = GC.GetAllocatedBytesForCurrentThread();
            beginTxTotal += (after - before);

            before = GC.GetAllocatedBytesForCurrentThread();
            UpdateBuilder<BenchmarkPerson> builder = tx.UpdateById<BenchmarkPerson>(id);
            after = GC.GetAllocatedBytesForCurrentThread();
            updateByIdTotal += (after - before);

            before = GC.GetAllocatedBytesForCurrentThread();
            builder.Set(BenchmarkPersonMeta.Age, 31 + (i % 10));
            after = GC.GetAllocatedBytesForCurrentThread();
            setTotal += (after - before);

            before = GC.GetAllocatedBytesForCurrentThread();
            builder.Execute();
            after = GC.GetAllocatedBytesForCurrentThread();
            executeTotal += (after - before);

            before = GC.GetAllocatedBytesForCurrentThread();
            tx.Commit();
            after = GC.GetAllocatedBytesForCurrentThread();
            commitTotal += (after - before);

            before = GC.GetAllocatedBytesForCurrentThread();
            tx.Dispose();
            after = GC.GetAllocatedBytesForCurrentThread();
            disposeTotal += (after - before);
        }

        Console.WriteLine($"  BeginTransaction:  {beginTxTotal / iterations,6} bytes");
        Console.WriteLine($"  UpdateById<T>():   {updateByIdTotal / iterations,6} bytes");
        Console.WriteLine($"  Set():             {setTotal / iterations,6} bytes");
        Console.WriteLine($"  Execute():         {executeTotal / iterations,6} bytes");
        Console.WriteLine($"  Commit():          {commitTotal / iterations,6} bytes");
        Console.WriteLine($"  Dispose():         {disposeTotal / iterations,6} bytes");
        Console.WriteLine($"  --------------------------------");
        Console.WriteLine($"  Total:             {(beginTxTotal + updateByIdTotal + setTotal + executeTotal + commitTotal + disposeTotal) / iterations,6} bytes");
    }
}
