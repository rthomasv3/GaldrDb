using System;

namespace GaldrDb.SimulationTests.Core;

public class SimulationStats
{
    public long PageReads { get; set; }
    public long PageWrites { get; set; }
    public long PageFlushes { get; set; }
    public long WalReads { get; set; }
    public long WalWrites { get; set; }
    public long WalFlushes { get; set; }
    public long CrashCount { get; set; }
    public long FaultCount { get; set; }
    public long FaultsInjected { get; set; }
    public long ReadErrorsInjected { get; set; }
    public long WriteErrorsInjected { get; set; }
    public long PartialWritesInjected { get; set; }
    public long CorruptReadsInjected { get; set; }
    public long DocumentsInserted { get; set; }
    public long DocumentsRead { get; set; }
    public long DocumentsUpdated { get; set; }
    public long DocumentsDeleted { get; set; }
    public long TransactionsCommitted { get; set; }
    public long TransactionsAborted { get; set; }
    public long InvariantChecks { get; set; }
    public long OperationsExecuted { get; set; }

    public void Reset()
    {
        PageReads = 0;
        PageWrites = 0;
        PageFlushes = 0;
        WalReads = 0;
        WalWrites = 0;
        WalFlushes = 0;
        CrashCount = 0;
        FaultCount = 0;
        FaultsInjected = 0;
        ReadErrorsInjected = 0;
        WriteErrorsInjected = 0;
        PartialWritesInjected = 0;
        CorruptReadsInjected = 0;
        DocumentsInserted = 0;
        DocumentsRead = 0;
        DocumentsUpdated = 0;
        DocumentsDeleted = 0;
        TransactionsCommitted = 0;
        TransactionsAborted = 0;
        InvariantChecks = 0;
        OperationsExecuted = 0;
    }

    public override string ToString()
    {
        return $"Stats: Reads={PageReads}, Writes={PageWrites}, Flushes={PageFlushes}, " +
               $"WAL Reads={WalReads}, WAL Writes={WalWrites}, WAL Flushes={WalFlushes}, " +
               $"Crashes={CrashCount}, Faults={FaultsInjected}, " +
               $"ReadErrors={ReadErrorsInjected}, WriteErrors={WriteErrorsInjected}, " +
               $"PartialWrites={PartialWritesInjected}, CorruptReads={CorruptReadsInjected}, " +
               $"Inserts={DocumentsInserted}, Reads={DocumentsRead}, Updates={DocumentsUpdated}, Deletes={DocumentsDeleted}, " +
               $"Commits={TransactionsCommitted}, Aborts={TransactionsAborted}";
    }
}
