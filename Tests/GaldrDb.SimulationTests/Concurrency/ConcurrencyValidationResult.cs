namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencyValidationResult
{
    public bool IsValid { get; set; }
    public int ExpectedSum { get; set; }
    public int ActualSum { get; set; }
    public int SuccessfulIncrements { get; set; }
    public int ConflictsDetected { get; set; }
    public int RetriesPerformed { get; set; }
    public string ErrorMessage { get; set; }
}
