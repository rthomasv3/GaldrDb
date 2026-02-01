using System;
using System.Collections.Generic;

namespace GaldrDbConsole.StressTest;

public class StressTestResult
{
    public bool Success { get; set; }
    public StopReason StopReason { get; set; }
    public Exception FatalError { get; set; }
    public TimeSpan Duration { get; set; }

    public long TotalOperations { get; set; }
    public long SuccessfulOperations { get; set; }
    public long FailedOperations { get; set; }

    public long InsertsCompleted { get; set; }
    public long ReadsCompleted { get; set; }
    public long UpdatesCompleted { get; set; }
    public long DeletesCompleted { get; set; }

    public long InsertsFailed { get; set; }
    public long ReadsFailed { get; set; }
    public long UpdatesFailed { get; set; }
    public long DeletesFailed { get; set; }

    public long WriteConflicts { get; set; }
    public long Retries { get; set; }
    public long MaxRetriesExceeded { get; set; }

    public long ValidationErrors { get; set; }
    public long UnexpectedErrors { get; set; }

    public double OperationsPerSecond { get; set; }
    public Dictionary<string, LatencyStats> LatencyByOperation { get; set; }

    public int FinalDocumentCount { get; set; }
    public int ExpectedDocumentCount { get; set; }
    public bool StateVerified { get; set; }

    public void PrintSummary()
    {
        Console.WriteLine();
        Console.WriteLine("=== GaldrDb Stress Test Results ===");
        Console.WriteLine();

        string status = Success ? "PASSED" : "FAILED";
        Console.WriteLine($"Status: {status}");
        Console.WriteLine($"Stop Reason: {FormatStopReason()}");
        Console.WriteLine($"Duration: {Duration.TotalSeconds:F2}s");

        if (FatalError != null)
        {
            Console.WriteLine();
            Console.WriteLine($"Fatal Error: {FatalError.GetType().Name}");
            Console.WriteLine($"  {FatalError.Message}");
            Console.WriteLine($"  Stack trace:");
            Console.WriteLine($"  {FatalError.StackTrace}");
        }

        Console.WriteLine();
        Console.WriteLine("Operations Summary:");
        Console.WriteLine($"  Total:      {TotalOperations:N0}");
        Console.WriteLine($"  Successful: {SuccessfulOperations:N0} ({GetPercentage(SuccessfulOperations, TotalOperations):F1}%)");
        Console.WriteLine($"  Failed:     {FailedOperations:N0} ({GetPercentage(FailedOperations, TotalOperations):F1}%)");

        Console.WriteLine();
        Console.WriteLine("By Operation Type:");
        Console.WriteLine($"  Inserts: {InsertsCompleted:N0} completed, {InsertsFailed:N0} failed");
        Console.WriteLine($"  Reads:   {ReadsCompleted:N0} completed, {ReadsFailed:N0} failed");
        Console.WriteLine($"  Updates: {UpdatesCompleted:N0} completed, {UpdatesFailed:N0} failed");
        Console.WriteLine($"  Deletes: {DeletesCompleted:N0} completed, {DeletesFailed:N0} failed");

        Console.WriteLine();
        Console.WriteLine("Transaction Stats:");
        Console.WriteLine($"  Write Conflicts: {WriteConflicts:N0} (expected)");
        Console.WriteLine($"  Retries: {Retries:N0}");
        Console.WriteLine($"  Max Retries Exceeded: {MaxRetriesExceeded:N0}");

        Console.WriteLine();
        Console.WriteLine("Errors:");
        Console.WriteLine($"  Validation Errors: {ValidationErrors:N0}");
        Console.WriteLine($"  Unexpected Errors: {UnexpectedErrors:N0}");

        Console.WriteLine();
        Console.WriteLine("Performance:");
        Console.WriteLine($"  Throughput: {OperationsPerSecond:F1} ops/sec");

        if (LatencyByOperation != null && LatencyByOperation.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("  Latency (ms):");
            Console.WriteLine("  Operation  |   P50  |   P95  |   P99  |   Avg  |   Min  |   Max");
            Console.WriteLine("  -----------|--------|--------|--------|--------|--------|--------");

            foreach (KeyValuePair<string, LatencyStats> kvp in LatencyByOperation)
            {
                string op = kvp.Key.PadRight(10);
                LatencyStats s = kvp.Value;
                Console.WriteLine($"  {op} | {s.P50,6:F2} | {s.P95,6:F2} | {s.P99,6:F2} | {s.Average,6:F2} | {s.Min,6:F2} | {s.Max,6:F2}");
            }
        }

        Console.WriteLine();
        string verificationStatus = StateVerified ? "VERIFIED" : "MISMATCH";
        Console.WriteLine($"Final State: {FinalDocumentCount:N0} documents (expected: {ExpectedDocumentCount:N0}) - {verificationStatus}");
        Console.WriteLine();
    }

    private string FormatStopReason()
    {
        string result;

        switch (StopReason)
        {
            case StopReason.Timeout:
                result = $"Timeout ({Duration.TotalSeconds:F0}s reached)";
                break;
            case StopReason.OperationLimitReached:
                result = $"Operation limit reached ({TotalOperations:N0} ops)";
                break;
            case StopReason.FatalError:
                result = "Fatal error encountered";
                break;
            case StopReason.UserCancellation:
                result = "User cancelled (Ctrl+C)";
                break;
            case StopReason.ValidationFailure:
                result = "Validation failure (data corruption detected)";
                break;
            default:
                result = StopReason.ToString();
                break;
        }

        return result;
    }

    private static double GetPercentage(long part, long total)
    {
        double result = 0;

        if (total > 0)
        {
            result = (part * 100.0) / total;
        }

        return result;
    }
}

public enum StopReason
{
    Timeout,
    OperationLimitReached,
    FatalError,
    UserCancellation,
    ValidationFailure
}
