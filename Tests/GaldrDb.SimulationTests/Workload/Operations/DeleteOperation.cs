using System;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Workload.Operations;

public class DeleteOperation : Operation
{
    private readonly int _docId;

    public override string OperationType => "Delete";

    public DeleteOperation(string collectionName, int docId)
    {
        CollectionName = collectionName;
        _docId = docId;
    }

    public override OperationResult Execute(GaldrDbEngine.GaldrDb db, Transaction tx, SimulationState state)
    {
        OperationResult result;

        try
        {
            tx.DeleteById<TestDocument>(_docId);
            result = OperationResult.Succeeded(_docId);
        }
        catch (Exception ex)
        {
            result = OperationResult.Failed(ex.Message);
        }

        return result;
    }

    public int DocId => _docId;
}
