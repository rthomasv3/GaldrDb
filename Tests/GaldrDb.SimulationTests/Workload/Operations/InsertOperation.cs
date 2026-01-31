using System;
using GaldrDbEngine.Transactions;
using GaldrDb.SimulationTests.Core;

namespace GaldrDb.SimulationTests.Workload.Operations;

public class InsertOperation : Operation
{
    private readonly SimulationRandom _rng;
    private readonly int _minPayloadSize;
    private readonly int _maxPayloadSize;

    public override string OperationType => "Insert";

    public InsertOperation(string collectionName, SimulationRandom rng, int minPayloadSize = 100, int maxPayloadSize = 1000)
    {
        CollectionName = collectionName;
        _rng = rng;
        _minPayloadSize = minPayloadSize;
        _maxPayloadSize = maxPayloadSize;
    }

    public override OperationResult Execute(GaldrDbEngine.GaldrDb db, ITransaction tx, SimulationState state)
    {
        OperationResult result;

        try
        {
            int payloadSize = _rng.Next(_minPayloadSize, _maxPayloadSize + 1);
            TestDocument doc = TestDocument.Generate(_rng, payloadSize);

            int docId = tx.Insert(doc);
            byte[] contentHash = doc.ComputeHash();

            result = OperationResult.Succeeded(docId, contentHash);
        }
        catch (Exception ex)
        {
            result = OperationResult.Failed(ex.Message);
        }

        return result;
    }
}
