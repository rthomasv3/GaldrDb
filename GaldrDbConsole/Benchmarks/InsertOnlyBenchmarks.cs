using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InsertOnlyBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private GaldrDb _galdrDb;
    private int _nextId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbInsertBench_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");

        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions { UseWal = true });

        _nextId = 1000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _galdrDb?.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    [Benchmark(Description = "GaldrDb Insert")]
    public int GaldrDb_Insert()
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
