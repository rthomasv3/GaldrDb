using System;
using GaldrDbEngine.Transactions;

namespace GaldrDb.SimulationTests.Workload.Operations;

public class ReadOperation : Operation
{
    private readonly int _docId;

    public override string OperationType => "Read";

    public ReadOperation(string collectionName, int docId)
    {
        CollectionName = collectionName;
        _docId = docId;
    }

    public override OperationResult Execute(GaldrDbEngine.GaldrDb db, ITransaction tx, SimulationState state)
    {
        OperationResult result;

        try
        {
            TestDocument doc = tx.GetById<TestDocument>(_docId);

            if (doc == null)
            {
                result = OperationResult.Failed($"Document {_docId} not found");
            }
            else
            {
                byte[] contentHash = doc.ComputeHash();
                result = OperationResult.Succeeded(_docId, contentHash);
            }
        }
        catch (Exception ex)
        {
            result = OperationResult.Failed(ex.Message);
        }

        return result;
    }

    public int DocId => _docId;
}
