# PgBulk

High-performance bulk operations for PostgreSQL using [Npgsql](https://www.npgsql.org/) binary import (`COPY ... FROM STDIN BINARY`).

## Features

- **Bulk Insert** — fast binary import of large datasets
- **Bulk Merge (Upsert)** — insert or update using primary key or custom key columns
- **Bulk Sync** — replace table contents (delete + insert) with optional `WHERE` clause
- **EF Core integration** — extension methods on `DbContext` with automatic table/column mapping
- **Manual mapping** — use without EF Core by defining table mappings explicitly
- **CancellationToken** support throughout all async operations
- **Configurable timeout** — `CommandTimeout` property (default: `0` = infinite, appropriate for bulk workloads)
- Multi-target: `net8.0`, `net9.0`, `net10.0`

## Installation

```shell
dotnet add package PgBulk.EFCore
```

Or for usage without EF Core:

```shell
dotnet add package PgBulk
```

## Quick Start (EF Core)

```csharp
using PgBulk.EFCore;

// Bulk insert
await dbContext.BulkInsertAsync(entities);

// Bulk upsert (merge) — matches on primary key
await dbContext.BulkMergeAsync(entities);

// Bulk sync — deletes rows not in the collection, then inserts all
await dbContext.BulkSyncAsync(entities);

// With options
await dbContext.BulkInsertAsync(entities,
    timeoutOverride: 120,
    onConflictIgnore: true,
    cancellationToken: ct);
```

## Quick Start (Manual Mapping)

```csharp
var provider = new ManualTableInformationProvider()
    .AddTableMapping<MyEntity>("my_table", c => c.Automap());

var bulk = new ManualBulkOperator(connectionString, provider);
await bulk.InsertAsync(entities, onConflictIgnore: false);
```

## Custom Merge Keys

By default, merge uses the primary key. To merge on different columns:

```csharp
var keyProvider = new EntityManualTableKeyProvider<MyEntity>();
await keyProvider.AddKeyColumn(e => e.ExternalId, dbContext);

await dbContext.BulkMergeAsync(entities, tableKeyProvider: keyProvider);
```

## Timeout Configuration

The default `CommandTimeout` is `0` (infinite), which is appropriate for bulk operations on large datasets. Override per-call or per-operator:

```csharp
// Per-call via extension method
await dbContext.BulkInsertAsync(entities, timeoutOverride: 300);

// Per-operator instance
var op = dbContext.GetBulkOperator(timeoutOverride: 300);
// or
op.CommandTimeout = 300;
```

## License

[MIT](LICENSE)
