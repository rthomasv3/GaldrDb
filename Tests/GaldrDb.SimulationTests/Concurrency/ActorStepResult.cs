namespace GaldrDb.SimulationTests.Concurrency;

public class ActorStepResult
{
    public bool OperationCompleted { get; set; }
    public bool ConflictOccurred { get; set; }
    public bool Success { get; set; }
    public int IncrementApplied { get; set; }
    public bool NeedsMoreSteps { get; set; }

    public static ActorStepResult Continue()
    {
        return new ActorStepResult
        {
            OperationCompleted = false,
            ConflictOccurred = false,
            Success = false,
            IncrementApplied = 0,
            NeedsMoreSteps = true
        };
    }

    public static ActorStepResult Completed(int incrementApplied)
    {
        return new ActorStepResult
        {
            OperationCompleted = true,
            ConflictOccurred = false,
            Success = true,
            IncrementApplied = incrementApplied,
            NeedsMoreSteps = false
        };
    }

    public static ActorStepResult Conflict()
    {
        return new ActorStepResult
        {
            OperationCompleted = false,
            ConflictOccurred = true,
            Success = false,
            IncrementApplied = 0,
            NeedsMoreSteps = true
        };
    }

    public static ActorStepResult Failed()
    {
        return new ActorStepResult
        {
            OperationCompleted = true,
            ConflictOccurred = false,
            Success = false,
            IncrementApplied = 0,
            NeedsMoreSteps = false
        };
    }
}
