using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using GaldrDbEngine;

namespace GaldrDbConsole.StressTest;

public class StressTestRunner
{
    private readonly StressTestConfiguration _config;
    private readonly StressTestState _state;
    private readonly StressTestStatistics _stats;
    private readonly List<StressTestWorker> _workers;
    private readonly List<Task> _workerTasks;
    private readonly CancellationTokenSource _cts;

    private GaldrDb _db;
    private string _testDirectory;
    private string _dbPath;
    private Stopwatch _stopwatch;
    private volatile bool _userCancelled;

    public StressTestRunner(StressTestConfiguration config)
    {
        _config = config;
        _state = new StressTestState();
        _stats = new StressTestStatistics();
        _workers = new List<StressTestWorker>();
        _workerTasks = new List<Task>();
        _cts = new CancellationTokenSource();
        _userCancelled = false;
    }

    public StressTestResult Run()
    {
        StressTestResult result;

        try
        {
            PrintHeader();
            Initialize();
            CreateDatabase();
            SeedInitialDocuments();
            RegisterCtrlCHandler();
            StartWorkers();
            WaitForCompletion();
            StopWorkers();
            result = ValidateAndReport();
        }
        catch (Exception ex)
        {
            result = new StressTestResult
            {
                Success = false,
                StopReason = StopReason.FatalError,
                FatalError = ex,
                Duration = _stopwatch?.Elapsed ?? TimeSpan.Zero
            };
        }
        finally
        {
            Cleanup();
        }

        return result;
    }

    private void PrintHeader()
    {
        Console.WriteLine("GaldrDb Stress Test");
        Console.WriteLine($"  Workload: {_config.Workload}");
        Console.WriteLine($"  Workers: {_config.WorkerCount}");
        Console.WriteLine($"  Timeout: {_config.TimeoutSeconds}s");
        if (_config.OperationLimit > 0)
        {
            Console.WriteLine($"  Operation Limit: {_config.OperationLimit:N0}");
        }
        Console.WriteLine($"  Initial docs: {_config.InitialDocumentCount}");
        Console.WriteLine($"  Seed: {_config.Seed}");
        Console.WriteLine();
    }

    private void Initialize()
    {
        if (string.IsNullOrEmpty(_config.DatabasePath))
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbStress_{Guid.NewGuid()}");
            Directory.CreateDirectory(_testDirectory);
            _dbPath = Path.Combine(_testDirectory, "stress.galdr");
        }
        else
        {
            _dbPath = _config.DatabasePath;
            _testDirectory = null;
        }

        if (_config.Verbose)
        {
            Console.WriteLine($"Database path: {_dbPath}");
        }
    }

    private void CreateDatabase()
    {
        GaldrDbOptions options = new GaldrDbOptions
        {
            UseWal = true,
            AutoCheckpoint = true,
            AutoGarbageCollection = true,
            PageCacheSize = 0
        };

        if (string.IsNullOrEmpty(_config.DatabasePath))
        {
            _db = GaldrDb.Create(_dbPath, options);
        }
        else
        {
            _db = GaldrDb.OpenOrCreate(_dbPath, options);
        }

        if (_config.Verbose)
        {
            Console.WriteLine("Database created/opened successfully.");
        }
    }

    private void SeedInitialDocuments()
    {
        Console.WriteLine($"Seeding {_config.InitialDocumentCount} initial documents...");

        Random rng = new Random(_config.Seed);

        for (int i = 0; i < _config.InitialDocumentCount; i++)
        {
            StressTestDocument doc = StressTestDocument.Generate(rng, -1);
            int docId = _db.Insert(doc);
            doc.Id = docId;

            byte[] hash = doc.ComputeHash();
            _state.RecordInsert(docId, hash, doc.Version);
        }

        Console.WriteLine($"Seeded {_config.InitialDocumentCount} documents.");
        Console.WriteLine();
    }

    private void RegisterCtrlCHandler()
    {
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            _userCancelled = true;
            _cts.Cancel();

            if (_config.Verbose)
            {
                Console.WriteLine();
                Console.WriteLine("Ctrl+C received, stopping workers...");
            }
        };
    }

    private void StartWorkers()
    {
        Console.WriteLine("Running...");

        if (_config.Verbose)
        {
            Console.WriteLine();
        }

        _stopwatch = Stopwatch.StartNew();
        WorkloadWeights weights = WorkloadWeights.FromProfile(_config.Workload);

        for (int i = 0; i < _config.WorkerCount; i++)
        {
            StressTestWorker worker = new StressTestWorker(
                i,
                _db,
                _state,
                _stats,
                weights,
                _config.MaxRetries,
                _config.Seed,
                _cts.Token);

            _workers.Add(worker);

            Task task = Task.Run(() => worker.Run());
            _workerTasks.Add(task);

            if (_config.Verbose)
            {
                Console.WriteLine($"[{_stopwatch.Elapsed:hh\\:mm\\:ss}] Worker {i} started");
            }
        }
    }

    private void WaitForCompletion()
    {
        DateTime startTime = DateTime.UtcNow;
        TimeSpan timeout = TimeSpan.FromSeconds(_config.TimeoutSeconds);
        TimeSpan progressInterval = TimeSpan.FromSeconds(5);
        DateTime lastProgressTime = DateTime.UtcNow;

        while (!_cts.Token.IsCancellationRequested)
        {
            if (DateTime.UtcNow - startTime >= timeout)
            {
                break;
            }

            if (_config.OperationLimit > 0 && _stats.TotalOperations >= _config.OperationLimit)
            {
                break;
            }

            bool anyFatalError = false;
            bool anyValidationError = false;

            foreach (StressTestWorker worker in _workers)
            {
                if (worker.HasFatalError)
                {
                    anyFatalError = true;
                    break;
                }
                if (worker.HasValidationError)
                {
                    anyValidationError = true;
                    break;
                }
            }

            if (anyFatalError || anyValidationError)
            {
                break;
            }

            if (_config.Verbose && DateTime.UtcNow - lastProgressTime >= progressInterval)
            {
                PrintProgress();
                lastProgressTime = DateTime.UtcNow;
            }

            Thread.Sleep(100);
        }
    }

    private void PrintProgress()
    {
        double elapsed = _stopwatch.Elapsed.TotalSeconds;
        double opsPerSec = elapsed > 0 ? _stats.TotalOperations / elapsed : 0;

        Console.WriteLine($"[{_stopwatch.Elapsed:hh\\:mm\\:ss}] Progress: {_stats.TotalOperations:N0} ops ({opsPerSec:F0} ops/sec) | " +
                          $"I:{_stats.InsertsCompleted} R:{_stats.ReadsCompleted} U:{_stats.UpdatesCompleted} D:{_stats.DeletesCompleted} | " +
                          $"Conflicts:{_stats.WriteConflicts}");
    }

    private void StopWorkers()
    {
        _cts.Cancel();

        foreach (StressTestWorker worker in _workers)
        {
            worker.Stop();
        }

        try
        {
            Task.WaitAll(_workerTasks.ToArray(), TimeSpan.FromSeconds(10));
        }
        catch (AggregateException)
        {
        }

        _stopwatch.Stop();
    }

    private StressTestResult ValidateAndReport()
    {
        StressTestResult result = new StressTestResult();

        result.Duration = _stopwatch.Elapsed;
        result.TotalOperations = _stats.TotalOperations;
        result.SuccessfulOperations = _stats.TotalSuccessful;
        result.FailedOperations = _stats.TotalFailed;

        result.InsertsCompleted = _stats.InsertsCompleted;
        result.ReadsCompleted = _stats.ReadsCompleted;
        result.UpdatesCompleted = _stats.UpdatesCompleted;
        result.DeletesCompleted = _stats.DeletesCompleted;

        result.InsertsFailed = _stats.InsertsFailed;
        result.ReadsFailed = _stats.ReadsFailed;
        result.UpdatesFailed = _stats.UpdatesFailed;
        result.DeletesFailed = _stats.DeletesFailed;

        result.WriteConflicts = _stats.WriteConflicts;
        result.Retries = _stats.RetriesPerformed;
        result.MaxRetriesExceeded = _stats.MaxRetriesExceeded;

        result.ValidationErrors = _stats.ValidationErrors;
        result.UnexpectedErrors = _stats.UnexpectedErrors;

        double elapsed = result.Duration.TotalSeconds;
        result.OperationsPerSecond = elapsed > 0 ? result.TotalOperations / elapsed : 0;

        result.LatencyByOperation = new Dictionary<string, LatencyStats>
        {
            { "Insert", _stats.GetLatencyStats("insert") },
            { "Read", _stats.GetLatencyStats("read") },
            { "Update", _stats.GetLatencyStats("update") },
            { "Delete", _stats.GetLatencyStats("delete") }
        };

        result.ExpectedDocumentCount = _state.DocumentCount;
        result.FinalDocumentCount = GetActualDocumentCount();
        result.StateVerified = result.FinalDocumentCount == result.ExpectedDocumentCount;

        Exception fatalError = null;
        int validationErrorDocId = -1;

        foreach (StressTestWorker worker in _workers)
        {
            if (worker.HasFatalError && fatalError == null)
            {
                fatalError = worker.FatalError;
            }
            if (worker.HasValidationError && validationErrorDocId < 0)
            {
                validationErrorDocId = worker.ValidationErrorDocId;
            }
        }

        result.FatalError = fatalError;

        if (_userCancelled)
        {
            result.StopReason = StopReason.UserCancellation;
            result.Success = result.ValidationErrors == 0 && result.UnexpectedErrors == 0;
        }
        else if (fatalError != null)
        {
            result.StopReason = StopReason.FatalError;
            result.Success = false;
        }
        else if (validationErrorDocId >= 0)
        {
            result.StopReason = StopReason.ValidationFailure;
            result.Success = false;
        }
        else if (_config.OperationLimit > 0 && _stats.TotalOperations >= _config.OperationLimit)
        {
            result.StopReason = StopReason.OperationLimitReached;
            result.Success = result.StateVerified && result.ValidationErrors == 0 && result.UnexpectedErrors == 0;
        }
        else
        {
            result.StopReason = StopReason.Timeout;
            result.Success = result.StateVerified && result.ValidationErrors == 0 && result.UnexpectedErrors == 0;
        }

        return result;
    }

    private int GetActualDocumentCount()
    {
        int count = 0;

        try
        {
            List<StressTestDocument> docs = _db.Query<StressTestDocument>().ToList();
            count = docs.Count;

            int queryCount = _db.Query<StressTestDocument>().Count();

            if (queryCount != count)
            {
                Console.WriteLine($"Warning: Document count mismatch. Query returned {queryCount}, but actual count is {count}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GetActualDocumentCount failed: {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
            count = -1;
        }

        return count;
    }

    private void Cleanup()
    {
        if (_db != null)
        {
            try
            {
                _db.Dispose();
            }
            catch
            {
            }
        }

        if (!_config.KeepDatabase && _testDirectory != null && Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
            }
        }

        _cts.Dispose();
    }
}
