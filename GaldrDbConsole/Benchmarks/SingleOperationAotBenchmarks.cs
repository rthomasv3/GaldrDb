using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using GaldrDbEngine.Utilities;
using Microsoft.Data.Sqlite;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
// [SimpleJob(RuntimeMoniker.Net10_0, warmupCount: 3, iterationCount: 25, invocationCount: 16384)]
[SimpleJob(RuntimeMoniker.NativeAot10_0, warmupCount: 3, iterationCount: 25, invocationCount: 16384)]
public class SingleOperationAotBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private string _sqlitePath;
    private GaldrDb _galdrDb;
    private SqliteConnection _sqliteConnection;
    private int _nextId;
    private int _existingId;
    private int _iterationCount;

    private const bool ENABLE_PERF_TRACING = false;
    private const int PRINT_EVERY_N_ITERATIONS = 4;

    private const bool MANUAL_CHECKPOINT = false;
    private const int CHECKPOINT_ITERATIONS = 2;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbAotBenchmarks_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");
        _sqlitePath = Path.Combine(_testDirectory, "benchmark.sqlite");

        SetupGaldrDb();
        SetupSqliteAdo();

        _nextId = 1000;
    }

    private void SetupGaldrDb()
    {
        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions
        {
            UseWal = true, 
            AutoCheckpoint = !MANUAL_CHECKPOINT,
        });

        _existingId = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "Existing Person",
            Age = 30,
            Email = "existing@example.com",
            Address = "123 Main St",
            Phone = "555-1234"
        });

        PerfTracer.Enabled = ENABLE_PERF_TRACING;
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _iterationCount++;

        if (ENABLE_PERF_TRACING)
#pragma warning disable CS0162 // Unreachable code detected
        {
            PerfTracer.Reset();
        }
#pragma warning restore CS0162 // Unreachable code detected
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        if (ENABLE_PERF_TRACING && _iterationCount % PRINT_EVERY_N_ITERATIONS == 0)
        {
            Console.WriteLine($"\n=== Iteration {_iterationCount} (Documents: ~{_nextId}) ===");
            PerfTracer.PrintAggregated();
        }

        if (MANUAL_CHECKPOINT && _iterationCount % CHECKPOINT_ITERATIONS == 0)
        {
            _galdrDb.Checkpoint();
        }
    }

    private void SetupSqliteAdo()
    {
        string connectionString = $"Data Source={_sqlitePath}";
        _sqliteConnection = new SqliteConnection(connectionString);
        _sqliteConnection.Open();

        using (SqliteCommand walCmd = _sqliteConnection.CreateCommand())
        {
            walCmd.CommandText = "PRAGMA journal_mode=WAL;";
            walCmd.ExecuteNonQuery();
        }

        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS Person (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT,
                    Age INTEGER,
                    Email TEXT,
                    Address TEXT,
                    Phone TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_person_name ON Person(Name);
            ";
            cmd.ExecuteNonQuery();
        }

        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Person (Name, Age, Email, Address, Phone)
                VALUES ('Existing Person', 30, 'existing@example.com', '123 Main St', '555-1234')
            ";
            cmd.ExecuteNonQuery();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _galdrDb?.Dispose();
        _sqliteConnection?.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    #region Read Benchmarks

    [Benchmark(Description = "GaldrDb Read")]
    [BenchmarkCategory("Read")]
    public BenchmarkPerson GaldrDb_ReadById()
    {
        return _galdrDb.GetById<BenchmarkPerson>(_existingId);
    }
    
    [Benchmark(Description = "SQLite ADO.NET Read")]
    [BenchmarkCategory("Read")]
    public SqlitePerson SqliteAdo_ReadById()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = "SELECT Id, Name, Age, Email, Address, Phone FROM Person WHERE Id = @id";
            cmd.Parameters.AddWithValue("@id", 1);
    
            using (SqliteDataReader reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    return new SqlitePerson
                    {
                        Id = reader.GetInt32(0),
                        Name = reader.GetString(1),
                        Age = reader.GetInt32(2),
                        Email = reader.GetString(3),
                        Address = reader.GetString(4),
                        Phone = reader.GetString(5)
                    };
                }
            }
        }
    
        return null;
    }

    #endregion

    #region Insert Benchmarks

    [Benchmark(Description = "GaldrDb Insert")]
    [BenchmarkCategory("Insert")]
    public int GaldrDb_Insert()
    {
        return _galdrDb.Insert(new BenchmarkPerson
        {
            Name = $"Person {_nextId++}",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        });
    }

    [Benchmark(Description = "SQLite ADO.NET Insert")]
    [BenchmarkCategory("Insert")]
    public long SqliteAdo_Insert()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Person (Name, Age, Email, Address, Phone)
                VALUES (@name, @age, @email, @address, @phone);
                SELECT last_insert_rowid();
            ";
            cmd.Parameters.AddWithValue("@name", $"Person {_nextId++}");
            cmd.Parameters.AddWithValue("@age", 25);
            cmd.Parameters.AddWithValue("@email", "test@example.com");
            cmd.Parameters.AddWithValue("@address", "456 Oak Ave");
            cmd.Parameters.AddWithValue("@phone", "555-5678");
    
            return (long)cmd.ExecuteScalar();
        }
    }

    #endregion

    #region Update Benchmarks

    private int _updateIdGaldr = 10000;
    private long _updateIdAdo = 10000;
    
    [IterationSetup(Target = nameof(GaldrDb_Update))]
    public void SetupGaldrUpdate()
    {
        _updateIdGaldr = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "To Update",
            Age = 99,
            Email = "random@example.com",
            Address = "Update St",
            Phone = "555-0000"
        });
    }
    
    [Benchmark(Description = "GaldrDb Update")]
    [BenchmarkCategory("Update")]
    public bool GaldrDb_Update()
    {
        return _galdrDb.UpdateById<BenchmarkPerson>(_updateIdGaldr)
            .Set(BenchmarkPersonMeta.Age, 31)
            .Set(BenchmarkPersonMeta.Email, "updated@example.com")
            .Execute();
    }
    
    [IterationSetup(Target = nameof(SqliteAdo_Update))]
    public void SetupAdoUpdate()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Person (Name, Age, Email, Address, Phone)
                VALUES ('To Update', 99, 'random@example.com', 'Update St', '555-0000');
                SELECT last_insert_rowid();
            ";
            _updateIdAdo = (long)cmd.ExecuteScalar();
        }
    }
    
    [Benchmark(Description = "SQLite ADO.NET Update")]
    [BenchmarkCategory("Update")]
    public int SqliteAdo_Update()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                UPDATE Person
                SET Age = @age, Email = @email
                WHERE Id = @id
            ";
            cmd.Parameters.AddWithValue("@id", _updateIdAdo);
            cmd.Parameters.AddWithValue("@age", 31);
            cmd.Parameters.AddWithValue("@email", "updated@example.com");
    
            return cmd.ExecuteNonQuery();
        }
    }

    #endregion

    #region Delete Benchmarks

    private int _deleteIdGaldr = 10000;
    private long _deleteIdAdo = 10000;
    
    [IterationSetup(Target = nameof(GaldrDb_Delete))]
    public void SetupGaldrDelete()
    {
        _deleteIdGaldr = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "To Delete",
            Age = 99,
            Email = "delete@example.com",
            Address = "Delete St",
            Phone = "555-0000"
        });
    }
    
    [Benchmark(Description = "GaldrDb Delete")]
    [BenchmarkCategory("Delete")]
    public bool GaldrDb_Delete()
    {
        return _galdrDb.DeleteById<BenchmarkPerson>(_deleteIdGaldr);
    }
    
    [IterationSetup(Target = nameof(SqliteAdo_Delete))]
    public void SetupAdoDelete()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO Person (Name, Age, Email, Address, Phone)
                VALUES ('To Delete', 99, 'delete@example.com', 'Delete St', '555-0000');
                SELECT last_insert_rowid();
            ";
            _deleteIdAdo = (long)cmd.ExecuteScalar();
        }
    }
    
    [Benchmark(Description = "SQLite ADO.NET Delete")]
    [BenchmarkCategory("Delete")]
    public int SqliteAdo_Delete()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM Person WHERE Id = @id";
            cmd.Parameters.AddWithValue("@id", _deleteIdAdo);
    
            return cmd.ExecuteNonQuery();
        }
    }

    #endregion
}
