using System;

namespace GaldrDbConsole.StressTest;

public enum WorkloadProfile
{
    Balanced,
    WriteHeavy,
    ReadHeavy,
    HighChurn
}

public class WorkloadWeights
{
    public int InsertWeight { get; set; }
    public int ReadWeight { get; set; }
    public int UpdateWeight { get; set; }
    public int DeleteWeight { get; set; }

    public int TotalWeight()
    {
        return InsertWeight + ReadWeight + UpdateWeight + DeleteWeight;
    }

    public static WorkloadWeights FromProfile(WorkloadProfile profile)
    {
        WorkloadWeights weights;

        switch (profile)
        {
            case WorkloadProfile.Balanced:
                weights = new WorkloadWeights
                {
                    InsertWeight = 30,
                    ReadWeight = 30,
                    UpdateWeight = 25,
                    DeleteWeight = 15
                };
                break;

            case WorkloadProfile.WriteHeavy:
                weights = new WorkloadWeights
                {
                    InsertWeight = 50,
                    ReadWeight = 10,
                    UpdateWeight = 30,
                    DeleteWeight = 10
                };
                break;

            case WorkloadProfile.ReadHeavy:
                weights = new WorkloadWeights
                {
                    InsertWeight = 20,
                    ReadWeight = 60,
                    UpdateWeight = 15,
                    DeleteWeight = 5
                };
                break;

            case WorkloadProfile.HighChurn:
                weights = new WorkloadWeights
                {
                    InsertWeight = 40,
                    ReadWeight = 10,
                    UpdateWeight = 10,
                    DeleteWeight = 40
                };
                break;

            default:
                weights = FromProfile(WorkloadProfile.Balanced);
                break;
        }

        return weights;
    }
}
