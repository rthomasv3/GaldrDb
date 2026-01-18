namespace GaldrDb.SimulationTests.Concurrency;

public enum ActorState
{
    Idle,
    Starting,
    ReadingDocument,
    ModifyingDocument,
    Committing,
    Retrying,
    Completed
}
