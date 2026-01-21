using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class UpdateBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private GaldrDb _galdrDb;
    private int _existingId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbUpdateBenchmarks_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");

        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions { UseWal = true });

        _existingId = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "Existing Person",
            Age = 30,
            Email = "existing@example.com",
            Address = "123 Main St",
            Phone = "555-1234"
        });
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

    [Benchmark(Description = "GetById + Update<T>")]
    public bool FullUpdate()
    {
        BenchmarkPerson person = _galdrDb.GetById<BenchmarkPerson>(_existingId);
        person.Age = 31;
        return _galdrDb.Update(person);
    }

    [Benchmark(Description = "UpdateById")]
    public bool PartialUpdateById()
    {
        return _galdrDb.UpdateById<BenchmarkPerson>(_existingId)
            .Set(BenchmarkPersonMeta.Age, 31)
            .Execute();
    }
}
