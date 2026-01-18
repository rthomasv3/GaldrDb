namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencyConfiguration
{
    public int WriterCount { get; set; } = 4;
    public int ReaderCount { get; set; } = 4;
    public int InitialDocumentCount { get; set; } = 100;
    public int InitialCounterValue { get; set; } = 20;
    public int OperationsPerActor { get; set; } = 50;
    public int MaxRetries { get; set; } = 5;
    public int Seed { get; set; } = 12345;
    public SchedulingStrategy Strategy { get; set; } = SchedulingStrategy.Random;

    public static ConcurrencyConfiguration HighContention()
    {
        return new ConcurrencyConfiguration
        {
            WriterCount = 8,
            ReaderCount = 4,
            InitialDocumentCount = 10,
            OperationsPerActor = 100,
            MaxRetries = 5
        };
    }

    public static ConcurrencyConfiguration LowContention()
    {
        return new ConcurrencyConfiguration
        {
            WriterCount = 4,
            ReaderCount = 8,
            InitialDocumentCount = 1000,
            OperationsPerActor = 50,
            MaxRetries = 5
        };
    }

    public static ConcurrencyConfiguration TwoWritersOneDocument()
    {
        return new ConcurrencyConfiguration
        {
            WriterCount = 2,
            ReaderCount = 0,
            InitialDocumentCount = 1,
            OperationsPerActor = 1,
            MaxRetries = 5
        };
    }
}
