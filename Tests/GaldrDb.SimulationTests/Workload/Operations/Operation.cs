using System;
using GaldrDbEngine;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Workload.Operations;

public abstract class Operation
{
    public string CollectionName { get; set; }

    public abstract OperationResult Execute(GaldrDbEngine.GaldrDb db, Transaction tx, SimulationState state);

    public abstract string OperationType { get; }
}

public class OperationResult
{
    public bool Success { get; set; }
    public int? DocId { get; set; }
    public byte[] ContentHash { get; set; }
    public string ErrorMessage { get; set; }

    public static OperationResult Succeeded(int? docId = null, byte[] contentHash = null)
    {
        return new OperationResult
        {
            Success = true,
            DocId = docId,
            ContentHash = contentHash
        };
    }

    public static OperationResult Failed(string errorMessage)
    {
        return new OperationResult
        {
            Success = false,
            ErrorMessage = errorMessage
        };
    }
}
