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
public class DeleteOnlyBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private GaldrDb _galdrDb;
    private int _deleteId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbDeleteBench_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");

        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions { UseWal = true });
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

    [IterationSetup]
    public void IterationSetup()
    {
        _deleteId = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "To Delete",
            Age = 99,
            Email = "delete@example.com",
            Address = "Delete St",
            Phone = "555-0000"
        });
    }

    [Benchmark(Description = "GaldrDb Delete")]
    public bool GaldrDb_Delete()
    {
        return _galdrDb.Delete<BenchmarkPerson>(_deleteId);
    }
}
