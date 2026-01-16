using System;
using System.Collections.Generic;
using GaldrDb.SimulationTests.Core;
using GaldrDb.SimulationTests.Workload.Operations;

namespace GaldrDb.SimulationTests.Workload;

public class WorkloadGenerator
{
    private readonly WorkloadConfiguration _config;
    private readonly SimulationRandom _rng;
    private readonly List<string> _collectionNames;

    public WorkloadGenerator(WorkloadConfiguration config, SimulationRandom rng)
    {
        _config = config;
        _rng = rng;
        _collectionNames = new List<string>();

        for (int i = 0; i < _config.CollectionCount; i++)
        {
            _collectionNames.Add($"TestCollection{i}");
        }
    }

    public Operation GenerateOperation(SimulationState state)
    {
        Operation result;
        string collection = _collectionNames[_rng.Next(_collectionNames.Count)];
        int totalWeight = _config.TotalWeight();
        int roll = _rng.Next(totalWeight);

        if (roll < _config.InsertWeight)
        {
            result = new InsertOperation(collection, _rng, _config.MinPayloadSize, _config.MaxPayloadSize);
        }
        else if (roll < _config.InsertWeight + _config.ReadWeight)
        {
            int? docId = state.GetRandomDocumentId(collection, _rng);
            if (docId.HasValue)
            {
                result = new ReadOperation(collection, docId.Value);
            }
            else
            {
                // No documents to read, do an insert instead
                result = new InsertOperation(collection, _rng, _config.MinPayloadSize, _config.MaxPayloadSize);
            }
        }
        else if (roll < _config.InsertWeight + _config.ReadWeight + _config.UpdateWeight)
        {
            int? docId = state.GetRandomDocumentId(collection, _rng);
            if (docId.HasValue)
            {
                result = new UpdateOperation(collection, docId.Value, _rng, _config.MinPayloadSize, _config.MaxPayloadSize);
            }
            else
            {
                // No documents to update, do an insert instead
                result = new InsertOperation(collection, _rng, _config.MinPayloadSize, _config.MaxPayloadSize);
            }
        }
        else
        {
            int? docId = state.GetRandomDocumentId(collection, _rng);
            if (docId.HasValue)
            {
                result = new DeleteOperation(collection, docId.Value);
            }
            else
            {
                // No documents to delete, do an insert instead
                result = new InsertOperation(collection, _rng, _config.MinPayloadSize, _config.MaxPayloadSize);
            }
        }

        return result;
    }

    public IReadOnlyList<string> CollectionNames => _collectionNames;
}
