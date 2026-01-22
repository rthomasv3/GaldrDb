using System;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using GaldrDbConsole.Benchmarks;

namespace GaldrDbConsole;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return;
        }

        string command = args[0].ToLower();

        switch (command)
        {
            case "--benchmark":
            case "-b":
                RunBenchmarks(args);
                break;
            case "--test-serialize":
            case "-ts":
                SerializationTest.Run();
                break;
            case "--test-insert":
            case "-ti":
                InsertTest.Run();
                break;
            case "--test-alloc":
            case "-ta":
                AllocationTracingTest.Run();
                break;
            case "--test-perf":
            case "-tp":
                PerformanceTracingTest.Run();
                break;
            case "--test-diag":
            case "-td":
                DiagnosticTest.Run();
                break;
            case "--help":
            case "-h":
                PrintUsage();
                break;
            default:
                Console.WriteLine($"Unknown command: {command}");
                PrintUsage();
                break;
        }
    }

    static void RunBenchmarks(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Please specify a benchmark suite.");
            PrintBenchmarkSuites();
            return;
        }

        string suite = args[1].ToLower();
        bool quick = Array.Exists(args, a => a.ToLower() == "--quick" || a.ToLower() == "-q");

        IConfig config = quick
            ? DefaultConfig.Instance.WithOptions(ConfigOptions.DisableOptimizationsValidator)
            : DefaultConfig.Instance;

        switch (suite)
        {
            case "single":
                Console.WriteLine("Running single operation benchmarks...");
                BenchmarkRunner.Run<SingleOperationBenchmarks>(config);
                break;
            case "insert":
                Console.WriteLine("Running insert-only benchmarks...");
                BenchmarkRunner.Run<InsertOnlyBenchmarks>(config);
                break;
            case "delete":
                Console.WriteLine("Running delete-only benchmarks...");
                BenchmarkRunner.Run<DeleteOnlyBenchmarks>(config);
                break;
            case "serialize":
                Console.WriteLine("Running serialization comparison benchmarks...");
                BenchmarkRunner.Run<SerializationComparisonBenchmarks>(config);
                break;
            case "json":
                Console.WriteLine("Running JSON document benchmarks...");
                BenchmarkRunner.Run<JsonDocumentBenchmarks>(config);
                break;
            case "update":
                Console.WriteLine("Running update comparison benchmarks...");
                BenchmarkRunner.Run<UpdateBenchmarks>(config);
                break;
            case "aot":
                Console.WriteLine("Running AOT comparison benchmarks...");
                BenchmarkRunner.Run<SingleOperationAotBenchmarks>(config);
                break;
            case "all":
                Console.WriteLine("Running all benchmarks...");
                BenchmarkRunner.Run<SingleOperationBenchmarks>(config);
                break;
            default:
                Console.WriteLine($"Unknown benchmark suite: {suite}");
                PrintBenchmarkSuites();
                break;
        }
    }

    static void PrintUsage()
    {
        Console.WriteLine("GaldrDb Console - Performance Benchmarking Tool");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  GaldrDbConsole --benchmark <suite> [options]");
        Console.WriteLine("  GaldrDbConsole --help");
        Console.WriteLine();
        Console.WriteLine("Commands:");
        Console.WriteLine("  -b, --benchmark <suite>  Run benchmark suite");
        Console.WriteLine("  -h, --help               Show this help message");
        Console.WriteLine();
        PrintBenchmarkSuites();
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  -q, --quick              Run in quick mode (fewer iterations)");
    }

    static void PrintBenchmarkSuites()
    {
        Console.WriteLine("Available benchmark suites:");
        Console.WriteLine("  single      Single operation benchmarks (insert, read, update, delete)");
        Console.WriteLine("  insert      Insert-only benchmarks (isolated GaldrDb insert)");
        Console.WriteLine("  delete      Delete-only benchmarks (isolated GaldrDb delete)");
        Console.WriteLine("  serialize   Serialization comparison benchmarks");
        Console.WriteLine("  json        JSON document benchmarks (GaldrDocument vs System.Text.Json)");
        Console.WriteLine("  update      Update comparison benchmarks (Full Update vs UpdateById)");
        Console.WriteLine("  aot         AOT comparison benchmarks (GaldrDb vs SQLite, JIT vs NativeAOT)");
        Console.WriteLine("  query       Query benchmarks (Phase 2 - not yet implemented)");
        Console.WriteLine("  bulk        Bulk operation benchmarks (Phase 3 - not yet implemented)");
        Console.WriteLine("  concurrent  Concurrent operation benchmarks (Phase 4 - not yet implemented)");
        Console.WriteLine("  memory      Memory & WAL benchmarks (Phase 5 - not yet implemented)");
        Console.WriteLine("  all         Run all implemented benchmarks");
    }
}
