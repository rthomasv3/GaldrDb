using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace GaldrDbConsole.Models;

[Table("Person")]
[Index(nameof(Name))]
public class SqlitePerson
{
    [Key]
    public int Id { get; set; }

    public string Name { get; set; }

    public int Age { get; set; }

    public string Email { get; set; }

    public string Address { get; set; }

    public string Phone { get; set; }
}

public class BenchmarkDbContext : DbContext
{
    private readonly string _connectionString;

    public DbSet<SqlitePerson> People { get; set; }

    public BenchmarkDbContext(string connectionString)
    {
        _connectionString = connectionString;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite(_connectionString, options =>
        {
            options.CommandTimeout(60);
        });
    }

    public void EnableWalMode()
    {
        Database.ExecuteSqlRaw("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SqlitePerson>().ToTable("Person");
    }
}
