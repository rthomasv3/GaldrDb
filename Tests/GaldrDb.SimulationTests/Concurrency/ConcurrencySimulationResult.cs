using System;

namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencySimulationResult
{
    public int TotalStepsExecuted { get; set; }
    public int SuccessfulOperations { get; set; }
    public int FailedOperations { get; set; }
    public int ConflictsDetected { get; set; }
    public int RetriesPerformed { get; set; }
    public int MaxRetriesExceeded { get; set; }
    public int SuccessfulReads { get; set; }
    public TimeSpan Duration { get; set; }
}
