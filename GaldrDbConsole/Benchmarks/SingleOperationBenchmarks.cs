using System;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using GaldrDbConsole.Models;
using GaldrDbEngine;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace GaldrDbConsole.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SingleOperationBenchmarks
{
    private string _testDirectory;
    private string _galdrDbPath;
    private string _sqlitePath;
    private GaldrDb _galdrDb;
    private SqliteConnection _sqliteConnection;
    private BenchmarkDbContext _efContext;
    private int _nextId;
    private int _existingId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"GaldrDbBenchmarks_{Guid.NewGuid()}");
        Directory.CreateDirectory(_testDirectory);

        _galdrDbPath = Path.Combine(_testDirectory, "benchmark.galdr");
        _sqlitePath = Path.Combine(_testDirectory, "benchmark.sqlite");

        SetupGaldrDb();
        SetupSqliteAdo();
        SetupSqliteEf();

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

        // Enable WAL mode for fair comparison
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

    private void SetupSqliteEf()
    {
        string efPath = Path.Combine(_testDirectory, "benchmark_ef.sqlite");
        string connectionString = $"Data Source={efPath}";
        _efContext = new BenchmarkDbContext(connectionString);
        _efContext.Database.EnsureCreated();
        _efContext.EnableWalMode();

        _efContext.People.Add(new SqlitePerson
        {
            Name = "Existing Person",
            Age = 30,
            Email = "existing@example.com",
            Address = "123 Main St",
            Phone = "555-1234"
        });
        _efContext.SaveChanges();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _galdrDb?.Dispose();
        _sqliteConnection?.Dispose();
        _efContext?.Dispose();

        if (Directory.Exists(_testDirectory))
        {
            Directory.Delete(_testDirectory, true);
        }
    }

    #region Insert Benchmarks

    [Benchmark(Description = "GaldrDb Insert")]
    [BenchmarkCategory("Insert")]
    public int GaldrDb_Insert_Serialize()
    {
        int id = _galdrDb.Insert(new BenchmarkPerson
        {
            Name = $"Person {_nextId++}",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        });

        return id;
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
    
            long id = (long)cmd.ExecuteScalar();
            return id;
        }
    }
    
    [Benchmark(Description = "SQLite EF Core Insert")]
    [BenchmarkCategory("Insert")]
    public int SqliteEf_Insert()
    {
        SqlitePerson person = new SqlitePerson
        {
            Name = $"Person {_nextId++}",
            Age = 25,
            Email = "test@example.com",
            Address = "456 Oak Ave",
            Phone = "555-5678"
        };
    
        _efContext.People.Add(person);
        _efContext.SaveChanges();
    
        return person.Id;
    }

    #endregion

    #region Read By ID Benchmarks

    [Benchmark(Description = "GaldrDb Read")]
    [BenchmarkCategory("Read")]
    public BenchmarkPerson GaldrDb_ReadById()
    {
        BenchmarkPerson person = _galdrDb.GetById<BenchmarkPerson>(_existingId);
        return person;
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
                    SqlitePerson person = new SqlitePerson
                    {
                        Id = reader.GetInt32(0),
                        Name = reader.GetString(1),
                        Age = reader.GetInt32(2),
                        Email = reader.GetString(3),
                        Address = reader.GetString(4),
                        Phone = reader.GetString(5)
                    };
                    return person;
                }
            }
        }
    
        return null;
    }
    
    [Benchmark(Description = "SQLite EF Core Read")]
    [BenchmarkCategory("Read")]
    public SqlitePerson SqliteEf_ReadById()
    {
        // Use AsNoTracking to bypass change tracker cache and actually hit the database
        SqlitePerson person = _efContext.People
            .AsNoTracking()
            .FirstOrDefault(p => p.Id == 1);
        return person;
    }

    #endregion

    #region Update Benchmarks

    [Benchmark(Description = "GaldrDb Update")]
    [BenchmarkCategory("Update")]
    public bool GaldrDb_Update()
    {
        BenchmarkPerson person = new BenchmarkPerson
        {
            Id = _existingId,
            Name = "Updated Person",
            Age = 31,
            Email = "updated@example.com",
            Address = "789 Pine Rd",
            Phone = "555-9999"
        };

        bool result = _galdrDb.Update(person);
        return result;
    }

    [Benchmark(Description = "SQLite ADO.NET Update")]
    [BenchmarkCategory("Update")]
    public int SqliteAdo_Update()
    {
        using (SqliteCommand cmd = _sqliteConnection.CreateCommand())
        {
            cmd.CommandText = @"
                UPDATE Person 
                SET Name = @name, Age = @age, Email = @email, Address = @address, Phone = @phone
                WHERE Id = @id
            ";
            cmd.Parameters.AddWithValue("@id", 1);
            cmd.Parameters.AddWithValue("@name", "Updated Person");
            cmd.Parameters.AddWithValue("@age", 31);
            cmd.Parameters.AddWithValue("@email", "updated@example.com");
            cmd.Parameters.AddWithValue("@address", "789 Pine Rd");
            cmd.Parameters.AddWithValue("@phone", "555-9999");
    
            int rowsAffected = cmd.ExecuteNonQuery();
            return rowsAffected;
        }
    }
    
    [Benchmark(Description = "SQLite EF Core Update")]
    [BenchmarkCategory("Update")]
    public int SqliteEf_Update()
    {
        // Use ExecuteUpdate for direct SQL update without loading/tracking entity
        int result = _efContext.People
            .Where(p => p.Id == 1)
            .ExecuteUpdate(setters => setters
                .SetProperty(p => p.Name, "Updated Person")
                .SetProperty(p => p.Age, 31)
                .SetProperty(p => p.Email, "updated@example.com")
                .SetProperty(p => p.Address, "789 Pine Rd")
                .SetProperty(p => p.Phone, "555-9999"));
    
        return result;
    }

    #endregion

    #region Delete Benchmarks

    private int _deleteIdGaldr = 10000;
    private int _deleteIdAdo = 10000;
    private int _deleteIdEf = 10000;
    
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
        bool result = _galdrDb.Delete<BenchmarkPerson>(_deleteIdGaldr);
        return result;
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
            _deleteIdAdo = (int)(long)cmd.ExecuteScalar();
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
    
            int rowsAffected = cmd.ExecuteNonQuery();
            return rowsAffected;
        }
    }
    
    [IterationSetup(Target = nameof(SqliteEf_Delete))]
    public void SetupEfDelete()
    {
        SqlitePerson person = new SqlitePerson
        {
            Name = "To Delete",
            Age = 99,
            Email = "delete@example.com",
            Address = "Delete St",
            Phone = "555-0000"
        };
        _efContext.People.Add(person);
        _efContext.SaveChanges();
        _deleteIdEf = person.Id;
    }
    
    [Benchmark(Description = "SQLite EF Core Delete")]
    [BenchmarkCategory("Delete")]
    public int SqliteEf_Delete()
    {
        // Use ExecuteDelete for direct SQL delete without loading/tracking entity
        int result = _efContext.People
            .Where(p => p.Id == _deleteIdEf)
            .ExecuteDelete();
    
        return result;
    }

    #endregion
}
