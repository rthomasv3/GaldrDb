# GaldrDb

AOT-native single-file document database for .NET 10.0.

GaldrDb is a high-performance, type-safe document database that compiles to native code. It features MVCC with optimistic concurrency control, write-ahead logging, a fluent query API with source-generated metadata, and full ACID guarantees - all in a single file.

## Features

- **Type-safe CRUD** with source-generated metadata and compile-time validation
- **MVCC** (Multi-Version Concurrency Control) with snapshot isolation
- **Write-Ahead Logging (WAL)** for crash recovery and durability
- **Fluent query API** with LINQ-style filtering, sorting, and projections
- **Secondary indexes** with unique constraint support
- **Memory-mapped I/O** with automatic fallback to standard file I/O
- **Native AOT** compilation support (no reflection, no dynamic code generation)
- **Low-allocations** query execution with projections
- **Garbage collection** and vacuum/compact for space reclamation

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
    .First();
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

[GaldrDbCollection(Name = "customers")]
public class Customer
{
    public int Id { get; set; }
    [GaldrDbIndex(IsUnique = true)]
    public string Email { get; set; }
}
```

### Basic CRUD Operations

```csharp
// Insert
int id = db.Insert(new Person { Name = "Alice", Age = 30 });

// Get by ID
var person = db.GetById<Person>(id);

// Update
person.Age = 31;
db.Update(person);

// Delete
db.Delete<Person>(id);
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
    .Take(10)
    .ToList();
```

## Advanced Features

### Transactions

```csharp
using (var tx = db.BeginTransaction())
{
    var person = tx.GetById<Person>(id);
    person.Age++;
    tx.Update(person);
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
    [GaldrDbIndex(IsUnique = true)]
    public string Sku { get; set; }
}

var products = db.Query<Product>()
    .Where(ProductMeta.Category, FieldOp.Equals, "Electronics")
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

### Checkpoint Control

```csharp
// Manually checkpoint WAL to main database file
db.Checkpoint();
await db.CheckpointAsync();
```

## Architecture & Design Decisions

### Storage Engine

- **Page-based storage** (default 8KB pages) with efficient allocation tracking
- **Slotted document pages** allow documents to span multiple pages
- **B+ tree indexing** on document IDs for fast lookups and range scans
- **Free Space Map (FSM)** tracks available space in each page for optimal placement

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
    UseMmap = true,                       // Use memory-mapped files (with auto-fallback)
    WarmupOnOpen = true,                  // Warmup pools on database open
    JsonWriterBufferSize = 4096,          // Initial buffer size for JSON serialization
    JsonWriterPoolWarmupCount = 4,        // JSON writer pool warmup size
    AutoGarbageCollection = true,         // Automatically collect old versions
    GarbageCollectionThreshold = 250,     // Commits before auto-gc
};
```

## Performance Characteristics

- **Memory-mapped I/O** reduces system call overhead for read-heavy workloads
- **Buffer pooling** across all I/O operations minimizes GC pressure
- **MVCC overhead**: Writers create new versions, readers pay no blocking cost
- **WAL write-through**: Committed data is flushed before returning (configurable)

## Native AOT

GaldrDb is designed for Native AOT compatibility:

- No reflection or runtime type inspection
- Source generators create all metadata at compile time
- All queries use type-safe lambda expressions

## Testing

- **663 integration tests** covering CRUD, transactions, queries, ACID properties, and recovery scenarios
- **53 deterministic simulation tests** for concurrent operations, conflict resolution, and edge cases
- **Performance benchmarks** for single operations, bulk inserts, and query performance
- Test coverage includes: ACID compliance, WAL recovery, MVCC isolation, query planning, schema management

## Project Structure

- **GaldrDbEngine/** - Core database engine library
- **GaldrDbConsole/** - Console application for demos and benchmarking
- **GaldrDbSourceGenerators/** - Roslyn source generators for metadata generation
- **Tests/GaldrDb.UnitTests/** - Integration and unit tests
- **Tests/GaldrDb.SimulationTests/** - Deterministic simulation tests for concurrent scenarios
