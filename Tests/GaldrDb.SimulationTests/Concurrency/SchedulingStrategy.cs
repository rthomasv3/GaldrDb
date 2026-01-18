namespace GaldrDb.SimulationTests.Concurrency;

public enum SchedulingStrategy
{
    Random,
    RoundRobin,
    ConflictBiased
}
