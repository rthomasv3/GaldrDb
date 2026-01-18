using System.Collections.Generic;
using System.Linq;
using GaldrDb.SimulationTests.Core;

namespace GaldrDb.SimulationTests.Concurrency;

public class ConcurrencyScheduler
{
    private readonly List<ConcurrencyActor> _actors;
    private readonly SimulationRandom _rng;
    private readonly SchedulingStrategy _strategy;
    private int _roundRobinIndex;

    public ConcurrencyScheduler(SimulationRandom rng, SchedulingStrategy strategy)
    {
        _actors = new List<ConcurrencyActor>();
        _rng = rng;
        _strategy = strategy;
        _roundRobinIndex = 0;
    }

    public void RegisterActor(ConcurrencyActor actor)
    {
        _actors.Add(actor);
    }

    public ConcurrencyActor SelectNextActor()
    {
        List<ConcurrencyActor> activeActors = _actors
            .Where(a => a.HasPendingWork)
            .ToList();

        ConcurrencyActor result = null;

        if (activeActors.Count > 0)
        {
            switch (_strategy)
            {
                case SchedulingStrategy.Random:
                    result = SelectRandom(activeActors);
                    break;

                case SchedulingStrategy.RoundRobin:
                    result = SelectRoundRobin(activeActors);
                    break;

                case SchedulingStrategy.ConflictBiased:
                    result = SelectConflictBiased(activeActors);
                    break;

                default:
                    result = SelectRandom(activeActors);
                    break;
            }
        }

        return result;
    }

    private ConcurrencyActor SelectRandom(List<ConcurrencyActor> activeActors)
    {
        int index = _rng.Next(activeActors.Count);
        return activeActors[index];
    }

    private ConcurrencyActor SelectRoundRobin(List<ConcurrencyActor> activeActors)
    {
        _roundRobinIndex = _roundRobinIndex % activeActors.Count;
        ConcurrencyActor actor = activeActors[_roundRobinIndex];
        _roundRobinIndex++;
        return actor;
    }

    private ConcurrencyActor SelectConflictBiased(List<ConcurrencyActor> activeActors)
    {
        // Bias toward actors that are in the middle of operations (not idle)
        // This increases the chance of conflicts
        List<ConcurrencyActor> nonIdleActors = activeActors
            .Where(a => !a.IsIdle)
            .ToList();

        ConcurrencyActor result;

        if (nonIdleActors.Count > 0 && _rng.Next(100) < 70)
        {
            int index = _rng.Next(nonIdleActors.Count);
            result = nonIdleActors[index];
        }
        else
        {
            int index = _rng.Next(activeActors.Count);
            result = activeActors[index];
        }

        return result;
    }

    public bool HasActiveActors()
    {
        return _actors.Any(a => a.HasPendingWork);
    }

    public int TotalActors => _actors.Count;
    public int ActiveActorCount => _actors.Count(a => a.HasPendingWork);
}
