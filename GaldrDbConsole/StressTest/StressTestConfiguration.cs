using System;

namespace GaldrDbConsole.StressTest;

public class StressTestConfiguration
{
    public WorkloadProfile Workload { get; set; } = WorkloadProfile.Balanced;
    public int WorkerCount { get; set; } = DEFAULT_WORKER_COUNT;
    public int TimeoutSeconds { get; set; } = DEFAULT_TIMEOUT_SECONDS;
    public int OperationLimit { get; set; } = 0;
    public int MaxRetries { get; set; } = DEFAULT_MAX_RETRIES;
    public string DatabasePath { get; set; }
    public int Seed { get; set; } = DEFAULT_SEED;
    public int InitialDocumentCount { get; set; } = DEFAULT_INITIAL_DOCUMENTS;
    public bool KeepDatabase { get; set; } = false;
    public bool Verbose { get; set; } = false;

    private const int DEFAULT_TIMEOUT_SECONDS = 60;
    private const int DEFAULT_WORKER_COUNT = 4;
    private const int DEFAULT_MAX_RETRIES = 5;
    private const int DEFAULT_SEED = 12345;
    private const int DEFAULT_INITIAL_DOCUMENTS = 100;

    public static StressTestConfiguration FromArgs(string[] args)
    {
        StressTestConfiguration config = new StressTestConfiguration();
        int i = 1;

        while (i < args.Length)
        {
            string arg = args[i].ToLower();

            if (!arg.StartsWith("-"))
            {
                config.Workload = ParseWorkloadProfile(arg);
                i++;
                continue;
            }

            switch (arg)
            {
                case "-t":
                case "--timeout":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int timeout))
                    {
                        config.TimeoutSeconds = timeout;
                    }
                    break;

                case "-w":
                case "--workers":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int workers))
                    {
                        config.WorkerCount = Math.Max(1, workers);
                    }
                    break;

                case "-l":
                case "--limit":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int limit))
                    {
                        config.OperationLimit = limit;
                    }
                    break;

                case "-r":
                case "--retries":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int retries))
                    {
                        config.MaxRetries = Math.Max(1, retries);
                    }
                    break;

                case "-p":
                case "--path":
                    i++;
                    if (i < args.Length)
                    {
                        config.DatabasePath = args[i];
                    }
                    break;

                case "-s":
                case "--seed":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int seed))
                    {
                        config.Seed = seed;
                    }
                    break;

                case "-i":
                case "--initial":
                    i++;
                    if (i < args.Length && int.TryParse(args[i], out int initial))
                    {
                        config.InitialDocumentCount = Math.Max(1, initial);
                    }
                    break;

                case "-k":
                case "--keep":
                    config.KeepDatabase = true;
                    break;

                case "-v":
                case "--verbose":
                    config.Verbose = true;
                    break;
            }

            i++;
        }

        return config;
    }

    private static WorkloadProfile ParseWorkloadProfile(string value)
    {
        WorkloadProfile profile;

        switch (value.ToLower())
        {
            case "balanced":
                profile = WorkloadProfile.Balanced;
                break;
            case "writeheavy":
            case "write-heavy":
            case "write":
                profile = WorkloadProfile.WriteHeavy;
                break;
            case "readheavy":
            case "read-heavy":
            case "read":
                profile = WorkloadProfile.ReadHeavy;
                break;
            case "highchurn":
            case "high-churn":
            case "churn":
                profile = WorkloadProfile.HighChurn;
                break;
            default:
                profile = WorkloadProfile.Balanced;
                break;
        }

        return profile;
    }

    public static void PrintUsage()
    {
        Console.WriteLine("Stress Test Options:");
        Console.WriteLine("  [profile]              Workload profile: balanced, writeheavy, readheavy, highchurn");
        Console.WriteLine("  -t, --timeout <sec>    Timeout in seconds (default: 60)");
        Console.WriteLine("  -w, --workers <n>      Number of concurrent workers (default: 4)");
        Console.WriteLine("  -l, --limit <n>        Operation limit, 0 for unlimited (default: 0)");
        Console.WriteLine("  -r, --retries <n>      Max retries per conflict (default: 5)");
        Console.WriteLine("  -p, --path <path>      Database file path (default: temp directory)");
        Console.WriteLine("  -s, --seed <n>         Random seed for reproducibility (default: 12345)");
        Console.WriteLine("  -i, --initial <n>      Initial document count (default: 100)");
        Console.WriteLine("  -k, --keep             Keep database after test");
        Console.WriteLine("  -v, --verbose          Verbose progress output");
    }
}
