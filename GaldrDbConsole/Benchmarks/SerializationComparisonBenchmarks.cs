using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Storage;
using GaldrDbEngine.Utilities;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SerializationComparisonBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private string _galdrDbPathNoIndex;
    private GaldrDb _galdrDb;
    private GaldrDb _galdrDbNoIndex;
    private int _nextId;
    private int _nextIdNoIndex;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbBenchmarks_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");
        _galdrDbPathNoIndex = Path.Combine(_testDirectory, "benchmark_noindex.galdr");

        JsonWriterPool.ResetStats();
        ListPool<int>.ResetStats();
        ListPool<DocumentLocation>.ResetStats();
        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions { UseWal = true, AutoGarbageCollection = false});
        _galdrDbNoIndex = GaldrDb.Create(_galdrDbPathNoIndex, new GaldrDbOptions { UseWal = true, AutoGarbageCollection = false});

        Console.WriteLine($"[GlobalSetup] Pool stats after create: Hits={JsonWriterPool.Stats.Hits}, Misses={JsonWriterPool.Stats.Misses}");

        _nextId = 1000;
        _nextIdNoIndex = 1000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        Console.WriteLine($"[GlobalCleanup] JsonWriterPool: Hits={JsonWriterPool.Stats.Hits}, Misses={JsonWriterPool.Stats.Misses}");
        Console.WriteLine($"[GlobalCleanup] ListPool<int>: Hits={ListPool<int>.Stats.Hits}, Misses={ListPool<int>.Stats.Misses}, CapMismatch={ListPool<int>.Stats.CapacityMismatches}");
        Console.WriteLine($"[GlobalCleanup] ListPool<DocumentLocation>: Hits={ListPool<DocumentLocation>.Stats.Hits}, Misses={ListPool<DocumentLocation>.Stats.Misses}, CapMismatch={ListPool<DocumentLocation>.Stats.CapacityMismatches}");

        _galdrDb?.Dispose();
        _galdrDbNoIndex?.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    [Benchmark(Description = "Insert without secondary index")]
    public int InsertNoIndex()
    {
        int id = _galdrDbNoIndex.Insert(new BenchmarkPersonNoIndex
        {
            Name = $"Person {_nextIdNoIndex++}",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        });

        return id;
    }
    
    [Benchmark(Description = "Insert with secondary index")]
    public int InsertWithIndex()
    {
        int id = _galdrDb.Insert(new BenchmarkPerson
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
