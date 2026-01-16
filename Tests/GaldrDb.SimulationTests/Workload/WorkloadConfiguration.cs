using System;

namespace GaldrDb.SimulationTests.Workload;

public class WorkloadConfiguration
{
    /// <summary>
    /// Probability weights for each operation type (should sum to 100).
    /// </summary>
    public int InsertWeight { get; set; } = 40;
    public int ReadWeight { get; set; } = 30;
    public int UpdateWeight { get; set; } = 20;
    public int DeleteWeight { get; set; } = 10;

    /// <summary>
    /// Total number of operations to execute.
    /// </summary>
    public int OperationCount { get; set; } = 10000;

    /// <summary>
    /// Number of collections to use.
    /// </summary>
    public int CollectionCount { get; set; } = 3;

    /// <summary>
    /// Minimum document payload size in bytes.
    /// </summary>
    public int MinPayloadSize { get; set; } = 100;

    /// <summary>
    /// Maximum document payload size in bytes.
    /// </summary>
    public int MaxPayloadSize { get; set; } = 1000;

    /// <summary>
    /// Operations per transaction (1 = auto-commit each operation).
    /// </summary>
    public int OperationsPerTransaction { get; set; } = 1;

    /// <summary>
    /// Seed for the random number generator.
    /// </summary>
    public int Seed { get; set; } = 12345;

    /// <summary>
    /// Creates a balanced read/write workload.
    /// </summary>
    public static WorkloadConfiguration Balanced()
    {
        return new WorkloadConfiguration
        {
            InsertWeight = 30,
            ReadWeight = 30,
            UpdateWeight = 25,
            DeleteWeight = 15
        };
    }

    /// <summary>
    /// Creates a write-heavy workload.
    /// </summary>
    public static WorkloadConfiguration WriteHeavy()
    {
        return new WorkloadConfiguration
        {
            InsertWeight = 50,
            ReadWeight = 10,
            UpdateWeight = 30,
            DeleteWeight = 10
        };
    }

    /// <summary>
    /// Creates a read-heavy workload.
    /// </summary>
    public static WorkloadConfiguration ReadHeavy()
    {
        return new WorkloadConfiguration
        {
            InsertWeight = 20,
            ReadWeight = 60,
            UpdateWeight = 15,
            DeleteWeight = 5
        };
    }

    /// <summary>
    /// Creates a high-churn workload with lots of inserts and deletes.
    /// </summary>
    public static WorkloadConfiguration HighChurn()
    {
        return new WorkloadConfiguration
        {
            InsertWeight = 40,
            ReadWeight = 10,
            UpdateWeight = 10,
            DeleteWeight = 40
        };
    }

    public int TotalWeight()
    {
        return InsertWeight + ReadWeight + UpdateWeight + DeleteWeight;
    }
}
