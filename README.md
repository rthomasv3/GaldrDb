# GaldrDb

AOT-native single-file document database for .NET 8.0, 9.0, and 10.0.

GaldrDb is a high-performance, type-safe document database that compiles to native code. It features MVCC with optimistic concurrency control, write-ahead logging, a fluent query API with source-generated metadata, and full ACID guarantees - all in a single file.

## Features

- **Type-safe CRUD** with source-generated metadata and compile-time validation
- **MVCC** (Multi-Version Concurrency Control) with snapshot isolation
- **Write-Ahead Logging (WAL)** for crash recovery and durability
- **Fluent query API** with LINQ-style filtering, sorting, and projections
- **Secondary indexes** with unique constraints and compound (multi-field) indexes
- **Encryption at rest** with AES-256-GCM and per-page nonces
- **Memory-mapped I/O** with automatic fallback to standard file I/O
- **Native AOT** compilation support (no reflection, no dynamic code generation)
- **Low-allocations** query execution with projections
- **Garbage collection** and vacuum/compact for space reclamation
- **Dynamic API** for runtime schema flexibility with JSON documents
- **Nested document queries** for querying properties of embedded objects
- **Collection field queries** for matching elements within arrays

## Quick Start

```csharp
// Define a model
[GaldrDbCollection]
public class Book
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public string Title { get; set; }
    public string Author { get; set; }
}

// Create a database
var options = new GaldrDbOptions { PageSize = 8192, UseWal = true };
var db = GaldrDb.Create("books.db", options);

// Insert a document
int id = db.Insert(new Book { Title = "The Pragmatic Programmer", Author = "Andy Hunt" });

// Query documents
var book = db.Query<Book>()
    .Where(BookMeta.Title, FieldOp.Equals, "The Pragmatic Programmer")
    .FirstOrDefault();
```

## Getting Started

### Creating and Opening Databases

```csharp
// Create a new database
var db = GaldrDb.Create("database.db", new GaldrDbOptions { PageSize = 8192 });

// Open an existing database
var db = GaldrDb.Open("database.db");
```

### Defining Models

```csharp
[GaldrDbCollection]
public class Person
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public string Name { get; set; }
    public int Age { get; set; }
}

[GaldrDbCollection("customers")]
public class Customer
{
    public int Id { get; set; }
    [GaldrDbIndex(Unique = true)]
    public string Email { get; set; }
}
```

### Basic CRUD Operations

```csharp
// Insert
int id = db.Insert(new Person { Name = "Alice", Age = 30 });

// Get by ID
var person = db.GetById<Person>(id);

// Replace
person.Age = 31;
db.Replace(person);

// Delete
db.DeleteById<Person>(id);

// Partial update (update specific fields without loading the full document)
db.UpdateById<Person>(id)
    .Set(PersonMeta.Age, 32)
    .Execute();
```

### Querying

```csharp
// Filter
var adults = db.Query<Person>()
    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 18)
    .ToList();

// Range query
var people = db.Query<Person>()
    .WhereBetween(PersonMeta.Age, 25, 40)
    .ToList();

// Sort and limit
var results = db.Query<Person>()
    .OrderByDescending(PersonMeta.Age)
    .Limit(10)
    .ToList();

// In/NotIn queries
var selected = db.Query<Person>()
    .WhereIn(PersonMeta.Name, "Alice", "Bob", "Charlie")
    .ToList();

// Pagination
var page = db.Query<Person>()
    .OrderBy(PersonMeta.Name)
    .Skip(20)
    .Limit(10)
    .ToList();

// Check existence
bool hasAdults = db.Query<Person>()
    .Where(PersonMeta.Age, FieldOp.GreaterThanOrEqual, 18)
    .Any();

// Query explanation (shows execution plan)
var explanation = db.Query<Person>()
    .Where(PersonMeta.Age, FieldOp.GreaterThan, 21)
    .Explain();
```

### ASP.NET Core Integration

Install the `GaldrDbAspNetCore` package for dependency injection support:

```
dotnet add package GaldrDbAspNetCore
```

**Basic setup (single database):**

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGaldrDb(options =>
{
    options.FilePath = "app.db";
    options.OpenMode = GaldrDbOpenMode.OpenOrCreate;
});

var app = builder.Build();

// Inject IGaldrDb directly
app.MapGet("/users/{id}", (IGaldrDb db, int id) =>
{
    return db.GetById<User>(id);
});
```

**Multiple databases (named instances):**

```csharp
// Register named databases
builder.Services.AddGaldrDb(options =>
{
    options.FilePath = "users.db";
});

builder.Services.AddGaldrDb("orders", options =>
{
    options.FilePath = "orders.db";
});

// Access named instances via IGaldrDbFactory
app.MapGet("/orders/{id}", (IGaldrDbFactory factory, int id) =>
{
    IGaldrDb db = factory.Get("orders");
    return db.GetById<Order>(id);
});
```

## Advanced Features

### Transactions

```csharp
using (var tx = db.BeginTransaction())
{
    var person = tx.GetById<Person>(id);
    person.Age++;
    tx.Replace(person);
    tx.Commit();
}
```

### Secondary Indexes

```csharp
[GaldrDbCollection]
public class Product
{
    public int Id { get; set; }
    [GaldrDbIndex]
    public string Category { get; set; }
    [GaldrDbIndex(Unique = true)]
    public string Sku { get; set; }
}

var products = db.Query<Product>()
    .Where(ProductMeta.Category, FieldOp.Equals, "Electronics")
    .ToList();
```

### Compound Indexes

Compound indexes optimize queries that filter on multiple fields. Define them at the class level:

```csharp
[GaldrDbCollection]
[GaldrDbCompoundIndex("Status", "CreatedDate")]
[GaldrDbCompoundIndex("Category", "Priority")]
public class Order
{
    public int Id { get; set; }
    public string Status { get; set; }
    public DateTime CreatedDate { get; set; }
    public string Category { get; set; }
    public int Priority { get; set; }
}

// Uses the Status_CreatedDate compound index
var orders = db.Query<Order>()
    .Where(OrderMeta.Status, FieldOp.Equals, "Pending")
    .Where(OrderMeta.CreatedDate, FieldOp.GreaterThan, DateTime.Today.AddDays(-7))
    .ToList();

// Prefix queries also use the compound index
var pendingOrders = db.Query<Order>()
    .Where(OrderMeta.Status, FieldOp.Equals, "Pending")
    .ToList();
```

Compound indexes follow the leftmost-prefix rule: an index on `(A, B, C)` can serve queries on `A`, `A AND B`, or `A AND B AND C`. The query planner automatically selects the best index based on filter selectivity.

### Nested Document Queries

Query properties of embedded objects using generated metadata:

```csharp
[GaldrDbCollection]
public class Person
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Address Address { get; set; }
}

public class Address
{
    public string City { get; set; }
    public string Country { get; set; }
}

// Query nested properties
var results = db.Query<Person>()
    .Where(PersonMeta.Address.City, FieldOp.Equals, "Seattle")
    .ToList();
```

#### Indexed Nested Properties

Add `[GaldrDbIndex]` to nested class properties to create secondary indexes on nested paths:

```csharp
public class Address
{
    [GaldrDbIndex]
    public string City { get; set; }

    [GaldrDbIndex(Unique = true)]
    public string ZipCode { get; set; }

    public string State { get; set; }  // Not indexed
}

[GaldrDbCollection]
public class Person
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Address Address { get; set; }  // City and ZipCode are auto-indexed
}

// Uses secondary index on Address.City
var results = db.Query<Person>()
    .Where(PersonMeta.Address.City, FieldOp.Equals, "Seattle")
    .ToList();
```

Compound indexes can also include nested paths using dot notation:

```csharp
[GaldrDbCollection]
[GaldrDbCompoundIndex("Status", "Address.City")]  // Mix top-level and nested fields
public class Person
{
    public int Id { get; set; }
    public string Status { get; set; }
    public Address Address { get; set; }
}

// Uses compound index
var results = db.Query<Person>()
    .Where(PersonMeta.Status, FieldOp.Equals, "Active")
    .Where(PersonMeta.Address.City, FieldOp.Equals, "Seattle")
    .ToList();
```

Null handling follows standard database semantics: null nested values are excluded from equality queries, and multiple null values are allowed for unique indexes (NULL != NULL).

### Collection Field Queries

Query documents based on elements within arrays or lists:

```csharp
[GaldrDbCollection]
public class Order
{
    public int Id { get; set; }
    public List<OrderItem> Items { get; set; }
}

public class OrderItem
{
    public string ProductName { get; set; }
    public decimal Price { get; set; }
}

// Find orders containing items over $100
var results = db.Query<Order>()
    .WhereAny(OrderMeta.Items.Price, FieldOp.GreaterThan, 100m)
    .ToList();
```

### Projections

```csharp
[GaldrDbProjection(typeof(Person))]
public partial class PersonSummary
{
    public string Name { get; set; }
    public int Age { get; set; }
}

var summaries = db.Query<PersonSummary>()
    .ToList();
```

### Vacuum and Compact

```csharp
// Remove old versions and compact pages
var result = db.Vacuum();

// Create a compacted copy
var result = db.CompactTo("compacted.db");
```

### Encryption at Rest

```csharp
// Create an encrypted database
var options = new GaldrDbOptions
{
    Encryption = new EncryptionOptions
    {
        Password = "your-secret-password",
        KdfIterations = 500000,  // PBKDF2 iterations (higher = more secure, slower open)
    }
};
var db = GaldrDb.Create("encrypted.db", options);

// Open an encrypted database (same password required)
var db = GaldrDb.Open("encrypted.db", options);
```

Encryption uses AES-256-GCM with per-page nonces. The WAL file is also encrypted when encryption is enabled.

### Checkpoint Control

```csharp
// Manually checkpoint WAL to main database file
db.Checkpoint();
await db.CheckpointAsync();
```

### Dynamic API

For scenarios where types aren't known at compile time, use the dynamic API with JSON strings:

```csharp
// Insert with JSON
int id = db.InsertDynamic("people", """{"Name": "Alice", "Age": 30}""");

// Get by ID (returns JsonDocument)
JsonDocument doc = db.GetByIdDynamic("people", id);

// Replace
db.ReplaceDynamic("people", id, """{"Id": 1, "Name": "Alice", "Age": 31}""");

// Partial update
db.UpdateByIdDynamic("people", id)
    .Set("Age", 32)
    .Execute();

// Query
var results = db.QueryDynamic("people")
    .Where("Age", FieldOp.GreaterThan, 25)
    .OrderBy("Name")
    .ToList();

// Delete
db.DeleteByIdDynamic("people", id);
```

## Architecture & Design Decisions

### Storage Engine

- **Page-based storage** (default 8KB pages) with efficient allocation tracking
- **Slotted document pages** allow documents to span multiple pages
- **B+ tree indexing** on document IDs for fast lookups and range scans
- **Free Space Map (FSM)** tracks available space in each page for optimal placement
- **Page cache** with configurable size for frequently accessed pages

### Concurrency Model

- **MVCC (Multi-Version Concurrency Control)** provides snapshot isolation
- **Optimistic Concurrency Control (OCC)** with conflict detection at commit time
- No read locks - readers never block writers
- Writers proceed optimistically and retry only on conflicts
- Each transaction sees a consistent snapshot of the database

### Durability

- **Write-Ahead Logging (WAL)** frames all writes before applying to the main database
- **Frame-based logging** with sequential numbers, checksums, and commit flags
- **Crash recovery** replays committed transactions on startup
- **Automatic checkpointing** merges WAL changes to the main database

## Configuration Options

```csharp
var options = new GaldrDbOptions
{
    PageSize = 8192,                      // Page size (must be power of 2, >= 1024)
    UseWal = true,                        // Enable write-ahead logging
    AutoCheckpoint = true,                // Automatically checkpoint WAL
    WalCheckpointThreshold = 1000,        // Frames before auto-checkpoint
    UseMmap = false,                      // Use memory-mapped files (with auto-fallback)
    WarmupOnOpen = true,                  // Warmup pools on database open
    JsonWriterBufferSize = 4096,          // Initial buffer size for JSON serialization
    JsonWriterPoolWarmupCount = 4,        // JSON writer pool warmup size
    AutoGarbageCollection = true,         // Automatically collect old versions
    GarbageCollectionThreshold = 250,     // Commits before auto-gc
    ExpansionPageCount = 256,             // Pages to add on expansion (2MB with 8KB pages)
    PageCacheSize = 2000,                 // Max cached pages (16MB with 8KB pages)
    Encryption = new EncryptionOptions    // Optional encryption (null = disabled)
    {
        Password = "your-secret-password",
        KdfIterations = 500000,           // PBKDF2 iterations for key derivation
    },
};
```

## Performance Characteristics

- **Memory-mapped I/O** reduces system call overhead for read-heavy workloads
- **Buffer pooling** across all I/O operations minimizes GC pressure
- **MVCC overhead**: Writers create new versions, readers pay no blocking cost
- **WAL write-through**: Committed data is flushed before returning (configurable)
- **Async support**: All CRUD and query operations have async variants (`InsertAsync`, `ToListAsync`, etc.)

### Benchmarks

These are the current benchmark results from `SingleOperationAotBenchmarks` and `SingleOperationBenchmarks`. The results are of the current in development build, with future optimizations planned. Additional benchmarks with more advanced queries will be added at a later date.

#### Setup

BenchmarkDotNet v0.15.8, Linux Fedora Linux 43 (Workstation Edition)
AMD Ryzen AI 7 PRO 350 w/ Radeon 860M 0.62GHz, 1 CPU, 16 logical and 8 physical cores
.NET SDK 10.0.101

InvocationCount=16384  IterationCount=20  WarmupCount=3

#### SingleOperationAotBenchmarks

Native AOT comparison between GaldrDb and SQLite ADO.NET. Both use WAL mode with equivalent durability guarantees (synchronous=FULL). GaldrDb shows strong read and delete performance; write performance is an area of active optimization.

| Method                  | Mean        | Error     | StdDev    | Rank | Gen0   | Gen1   | Allocated |
|------------------------ |------------:|----------:|----------:|-----:|-------:|-------:|----------:|
| 'GaldrDb Delete'        |    272.9 ns |  11.28 ns |  12.99 ns |    1 | 0.0610 |      - |     713 B |
| 'GaldrDb Read'          |  1,795.1 ns |  79.36 ns |  84.92 ns |    2 | 0.1221 |      - |    1374 B |
| 'SQLite ADO.NET Delete' |  2,476.2 ns |  19.04 ns |  20.37 ns |    3 | 0.0610 |      - |     768 B |
| 'SQLite ADO.NET Update' |  3,146.7 ns |  15.25 ns |  16.95 ns |    4 | 0.1221 |      - |    1200 B |
| 'SQLite ADO.NET Read'   |  4,695.1 ns |  32.25 ns |  34.51 ns |    5 | 0.1221 |      - |    1280 B |
| 'SQLite ADO.NET Insert' | 10,076.2 ns | 302.63 ns | 348.51 ns |    6 | 0.2441 | 0.0610 |    2248 B |
| 'GaldrDb Update'        | 20,401.6 ns | 157.08 ns | 174.59 ns |    7 | 0.4272 | 0.0610 |    3982 B |
| 'GaldrDb Insert'        | 23,704.4 ns | 552.45 ns | 567.32 ns |    8 | 0.4272 | 0.1221 |    3935 B |

#### SingleOperationBenchmarks

JIT comparison including EF Core. GaldrDb outperforms EF Core across all operations while providing similar developer ergonomics. Note that EF Core Insert has significant per-operation overhead from change tracking and context management.

| Method                  | Mean           | Error        | StdDev       | Median         | Rank | Gen0     | Gen1    | Gen2   | Allocated |
|------------------------ |---------------:|-------------:|-------------:|---------------:|-----:|---------:|--------:|-------:|----------:|
| 'GaldrDb Delete'        |       701.2 ns |    174.53 ns |    179.23 ns |       786.3 ns |    1 |   0.0610 |       - |      - |     832 B |
| 'GaldrDb Read'          |     1,625.8 ns |     85.68 ns |     84.15 ns |     1,596.6 ns |    2 |   0.1221 |       - |      - |    1374 B |
| 'SQLite ADO.NET Delete' |     2,781.5 ns |    414.43 ns |    477.26 ns |     2,450.1 ns |    3 |   0.0610 |       - |      - |     768 B |
| 'SQLite ADO.NET Update' |     3,342.9 ns |     61.09 ns |     62.73 ns |     3,334.6 ns |    4 |   0.1221 |       - |      - |    1200 B |
| 'SQLite ADO.NET Read'   |     4,786.6 ns |     41.29 ns |     38.62 ns |     4,779.8 ns |    5 |   0.1221 |       - |      - |    1296 B |
| 'SQLite ADO.NET Insert' |     9,872.5 ns |    123.61 ns |    121.40 ns |     9,872.1 ns |    6 |   0.2441 |  0.0610 |      - |    2264 B |
| 'SQLite EF Core Delete' |    16,673.2 ns |    160.97 ns |    172.24 ns |    16,616.0 ns |    7 |   1.4648 |  0.4883 |      - |   12520 B |
| 'GaldrDb Update'        |    18,957.7 ns |    259.52 ns |    288.46 ns |    18,943.1 ns |    8 |   0.4272 |  0.0610 |      - |    4078 B |
| 'SQLite EF Core Read'   |    20,643.3 ns |    341.67 ns |    393.47 ns |    20,544.7 ns |    9 |   1.4648 |  0.4883 |      - |   12528 B |
| 'SQLite EF Core Update' |    20,906.7 ns |    201.73 ns |    215.85 ns |    20,906.7 ns |    9 |   1.8311 |  0.6104 |      - |   15704 B |
| 'GaldrDb Insert'        |    23,214.8 ns |    448.47 ns |    498.47 ns |    23,177.1 ns |   10 |   0.4272 |  0.1221 |      - |    4055 B |
| 'SQLite EF Core Insert' | 1,380,911.3 ns | 30,631.48 ns | 34,046.81 ns | 1,376,880.2 ns |   11 | 366.5161 | 19.8975 | 8.7891 | 3031581 B |

## Native AOT

GaldrDb is designed for Native AOT compatibility:

- No reflection or runtime type inspection
- Source generators create all metadata at compile time
- All queries use type-safe lambda expressions

## Testing

- **1047 unit and integration tests** covering CRUD, transactions, queries, ACID properties, and recovery scenarios
- **63 deterministic simulation tests** for concurrent operations, conflict resolution, and edge cases
- **1110 total tests** across the test suite
- **Performance benchmarks** for single operations, bulk inserts, and query performance
- Test coverage includes: ACID compliance, WAL recovery, MVCC isolation, query planning, schema management

## Project Structure

- **GaldrDbEngine/** - Core database engine library
- **GaldrDbBrowser/** - Cross-platform database browser for inspecting and editing data
- **GaldrDbConsole/** - Console application for demos and benchmarking
- **GaldrDbSourceGenerators/** - Roslyn source generators for metadata generation
- **Tests/GaldrDb.UnitTests/** - Integration and unit tests
- **Tests/GaldrDb.SimulationTests/** - Deterministic simulation tests for concurrent scenarios
