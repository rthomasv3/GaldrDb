using System;
using GaldrDbEngine.Transactions;
using GaldrDb.SimulationTests.Core;

namespace GaldrDb.SimulationTests.Workload.Operations;

public class UpdateOperation : Operation
{
    private readonly int _docId;
    private readonly SimulationRandom _rng;
    private readonly int _minPayloadSize;
    private readonly int _maxPayloadSize;

    public override string OperationType => "Update";

    public UpdateOperation(string collectionName, int docId, SimulationRandom rng, int minPayloadSize = 100, int maxPayloadSize = 1000)
    {
        CollectionName = collectionName;
        _docId = docId;
        _rng = rng;
        _minPayloadSize = minPayloadSize;
        _maxPayloadSize = maxPayloadSize;
    }

    public override OperationResult Execute(GaldrDbEngine.GaldrDb db, Transaction tx, SimulationState state)
    {
        OperationResult result;

        try
        {
            int payloadSize = _rng.Next(_minPayloadSize, _maxPayloadSize + 1);
            TestDocument doc = TestDocument.Generate(_rng, payloadSize);
            doc.Id = _docId;

            tx.Replace(doc);
            byte[] contentHash = doc.ComputeHash();

            result = OperationResult.Succeeded(_docId, contentHash);
        }
        catch (Exception ex)
        {
            result = OperationResult.Failed(ex.Message);
        }

        return result;
    }

    public int DocId => _docId;
}
