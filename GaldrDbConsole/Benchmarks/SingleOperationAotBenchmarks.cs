using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using GaldrDbEngine.Generated;
using Microsoft.Data.Sqlite;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
// [SimpleJob(RuntimeMoniker.Net10_0, warmupCount: 3, iterationCount: 15)]
// [SimpleJob(RuntimeMoniker.Net10_0)]
// [SimpleJob(RuntimeMoniker.NativeAot10_0, warmupCount: 3, iterationCount: 15)]
[SimpleJob(RuntimeMoniker.NativeAot10_0, warmupCount: 3, iterationCount: 50)]
public class SingleOperationAotBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private string _sqlitePath;
    private GaldrDb _galdrDb;
    private SqliteConnection _sqliteConnection;
    private int _nextId;
    private int _existingId;

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
        _galdrDb = GaldrDb.Create(_galdrDbPath, new GaldrDbOptions { UseWal = true });

        _existingId = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = "Existing Person",
            Age = 30,
            Email = "existing@example.com",
            Address = "123 Main St",
            Phone = "555-1234"
        });
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

    // [Benchmark(Description = "GaldrDb Read")]
    // [BenchmarkCategory("Read")]
    // public BenchmarkPerson GaldrDb_ReadById()
    // {
    //     return _galdrDb.GetById<BenchmarkPerson>(_existingId);
    // }
    //
    // [Benchmark(Description = "SQLite ADO.NET Read")]
    // [BenchmarkCategory("Read")]
    // public SqlitePerson SqliteAdo_ReadById()
    // {
    //     using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
    //     {
    //         cmd.CommandText = "SELECT Id, Name, Age, Email, Address, Phone FROM Person WHERE Id = @id";
    //         cmd.Parameters.AddWithValue("@id", 1);
    //
    //         using (SqliteDataReader reader = cmd.ExecuteReader())
    //         {
    //             if (reader.Read())
    //             {
    //                 return new SqlitePerson
    //                 {
    //                     Id = reader.GetInt32(0),
    //                     Name = reader.GetString(1),
    //                     Age = reader.GetInt32(2),
    //                     Email = reader.GetString(3),
    //                     Address = reader.GetString(4),
    //                     Phone = reader.GetString(5)
    //                 };
    //             }
    //         }
    //     }
    //
    //     return null;
    // }

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

    // [Benchmark(Description = "GaldrDb Update")]
    // [BenchmarkCategory("Update")]
    // public bool GaldrDb_Update()
    // {
    //     return _galdrDb.UpdateById<BenchmarkPerson>(_existingId)
    //         .Set(BenchmarkPersonMeta.Name, "Updated Person")
    //         .Set(BenchmarkPersonMeta.Age, 31)
    //         .Set(BenchmarkPersonMeta.Email, "updated@example.com")
    //         .Set(BenchmarkPersonMeta.Address, "789 Pine Rd")
    //         .Set(BenchmarkPersonMeta.Phone, "555-9999")
    //         .Execute();
    // }
    //
    // [Benchmark(Description = "SQLite ADO.NET Update")]
    // [BenchmarkCategory("Update")]
    // public int SqliteAdo_Update()
    // {
    //     using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
    //     {
    //         cmd.CommandText = @"
    //             UPDATE Person
    //             SET Name = @name, Age = @age, Email = @email, Address = @address, Phone = @phone
    //             WHERE Id = @id
    //         ";
    //         cmd.Parameters.AddWithValue("@id", 1);
    //         cmd.Parameters.AddWithValue("@name", "Updated Person");
    //         cmd.Parameters.AddWithValue("@age", 31);
    //         cmd.Parameters.AddWithValue("@email", "updated@example.com");
    //         cmd.Parameters.AddWithValue("@address", "789 Pine Rd");
    //         cmd.Parameters.AddWithValue("@phone", "555-9999");
    //
    //         return cmd.ExecuteNonQuery();
    //     }
    // }

    #endregion

    #region Delete Benchmarks

    // private int _deleteIdGaldr = 10000;
    // private int _deleteIdAdo = 10000;
    //
    // [IterationSetup(Target = nameof(GaldrDb_Delete))]
    // public void SetupGaldrDelete()
    // {
    //     _deleteIdGaldr = _galdrDb.Insert(new BenchmarkPerson
    //     {
    //         Name = "To Delete",
    //         Age = 99,
    //         Email = "delete@example.com",
    //         Address = "Delete St",
    //         Phone = "555-0000"
    //     });
    // }
    //
    // [Benchmark(Description = "GaldrDb Delete")]
    // [BenchmarkCategory("Delete")]
    // public bool GaldrDb_Delete()
    // {
    //     return _galdrDb.Delete<BenchmarkPerson>(_deleteIdGaldr);
    // }
    //
    // [IterationSetup(Target = nameof(SqliteAdo_Delete))]
    // public void SetupAdoDelete()
    // {
    //     using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
    //     {
    //         cmd.CommandText = @"
    //             INSERT INTO Person (Name, Age, Email, Address, Phone)
    //             VALUES ('To Delete', 99, 'delete@example.com', 'Delete St', '555-0000');
    //             SELECT last_insert_rowid();
    //         ";
    //         _deleteIdAdo = (int)(long)cmd.ExecuteScalar();
    //     }
    // }
    //
    // [Benchmark(Description = "SQLite ADO.NET Delete")]
    // [BenchmarkCategory("Delete")]
    // public int SqliteAdo_Delete()
    // {
    //     using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
    //     {
    //         cmd.CommandText = "DELETE FROM Person WHERE Id = @id";
    //         cmd.Parameters.AddWithValue("@id", _deleteIdAdo);
    //
    //         return cmd.ExecuteNonQuery();
    //     }
    // }

    #endregion
}
