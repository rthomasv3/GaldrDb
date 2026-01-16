namespace GaldrDbEngine.Utilities;

internal static class AllocationTracker
{
    private static long _validateTotal;
    private static long _validateCount;
    private static long _beginWalTotal;
    private static long _beginWalCount;
    private static long _commitInsertTotal;
    private static long _commitInsertCount;
    private static long _addVersionTotal;
    private static long _addVersionCount;
    private static long _commitWalTotal;
    private static long _commitWalCount;
    private static long _clearTotal;
    private static long _clearCount;
    private static long _gcTotal;
    private static long _gcCount;
    private static long _checkpointTotal;
    private static long _checkpointCount;

    public static void Reset()
    {
        _validateTotal = 0;
        _validateCount = 0;
        _beginWalTotal = 0;
        _beginWalCount = 0;
        _commitInsertTotal = 0;
        _commitInsertCount = 0;
        _addVersionTotal = 0;
        _addVersionCount = 0;
        _commitWalTotal = 0;
        _commitWalCount = 0;
        _clearTotal = 0;
        _clearCount = 0;
        _gcTotal = 0;
        _gcCount = 0;
        _checkpointTotal = 0;
        _checkpointCount = 0;
    }

    public static void RecordValidate(long bytes)
    {
        _validateTotal += bytes;
        _validateCount++;
    }

    public static void RecordBeginWal(long bytes)
    {
        _beginWalTotal += bytes;
        _beginWalCount++;
    }

    public static void RecordCommitInsert(long bytes)
    {
        _commitInsertTotal += bytes;
        _commitInsertCount++;
    }

    public static void RecordAddVersion(long bytes)
    {
        _addVersionTotal += bytes;
        _addVersionCount++;
    }

    public static void RecordCommitWal(long bytes)
    {
        _commitWalTotal += bytes;
        _commitWalCount++;
    }

    public static void RecordClear(long bytes)
    {
        _clearTotal += bytes;
        _clearCount++;
    }

    public static void RecordGC(long bytes)
    {
        _gcTotal += bytes;
        _gcCount++;
    }

    public static void RecordCheckpoint(long bytes)
    {
        _checkpointTotal += bytes;
        _checkpointCount++;
    }

    public static (long Total, long Count, long Avg) GetValidateStats()
    {
        long avg = _validateCount > 0 ? _validateTotal / _validateCount : 0;
        return (_validateTotal, _validateCount, avg);
    }

    public static (long Total, long Count, long Avg) GetBeginWalStats()
    {
        long avg = _beginWalCount > 0 ? _beginWalTotal / _beginWalCount : 0;
        return (_beginWalTotal, _beginWalCount, avg);
    }

    public static (long Total, long Count, long Avg) GetCommitInsertStats()
    {
        long avg = _commitInsertCount > 0 ? _commitInsertTotal / _commitInsertCount : 0;
        return (_commitInsertTotal, _commitInsertCount, avg);
    }

    public static (long Total, long Count, long Avg) GetAddVersionStats()
    {
        long avg = _addVersionCount > 0 ? _addVersionTotal / _addVersionCount : 0;
        return (_addVersionTotal, _addVersionCount, avg);
    }

    public static (long Total, long Count, long Avg) GetCommitWalStats()
    {
        long avg = _commitWalCount > 0 ? _commitWalTotal / _commitWalCount : 0;
        return (_commitWalTotal, _commitWalCount, avg);
    }

    public static (long Total, long Count, long Avg) GetClearStats()
    {
        long avg = _clearCount > 0 ? _clearTotal / _clearCount : 0;
        return (_clearTotal, _clearCount, avg);
    }

    public static (long Total, long Count, long Avg) GetGCStats()
    {
        long avg = _gcCount > 0 ? _gcTotal / _gcCount : 0;
        return (_gcTotal, _gcCount, avg);
    }

    public static (long Total, long Count, long Avg) GetCheckpointStats()
    {
        long avg = _checkpointCount > 0 ? _checkpointTotal / _checkpointCount : 0;
        return (_checkpointTotal, _checkpointCount, avg);
    }

    public static void PrintStats()
    {
        System.Console.WriteLine("=== Allocation Tracker Stats ===");

        (long total, long count, long avg) = GetValidateStats();
        System.Console.WriteLine($"Validate:     total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetBeginWalStats();
        System.Console.WriteLine($"BeginWal:     total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetCommitInsertStats();
        System.Console.WriteLine($"CommitInsert: total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetAddVersionStats();
        System.Console.WriteLine($"AddVersion:   total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetCommitWalStats();
        System.Console.WriteLine($"CommitWal:    total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetClearStats();
        System.Console.WriteLine($"Clear:        total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetGCStats();
        System.Console.WriteLine($"GC:           total={total}, count={count}, avg={avg} bytes");

        (total, count, avg) = GetCheckpointStats();
        System.Console.WriteLine($"Checkpoint:   total={total}, count={count}, avg={avg} bytes");
    }
}
