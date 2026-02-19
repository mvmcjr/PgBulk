using Microsoft.EntityFrameworkCore;
using PgBulk.Abstractions;

namespace PgBulk.EFCore;

public static class ContextExtensions
{
    public static Task BulkSyncAsync<T>(this DbContext dbContext, IEnumerable<T> entities, string? deleteWhere = null, int? timeoutOverride = null, bool useContextConnection = true, CancellationToken cancellationToken = default) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.SyncAsync(entities, deleteWhere, cancellationToken: cancellationToken);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = null, bool useContextConnection = true, ITableKeyProvider? tableKeyProvider = null, CancellationToken cancellationToken = default) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.MergeAsync(entities.ToList(), tableKeyProvider, cancellationToken);
    }

    public static Task BulkMergeAsync<T>(this DbContext dbContext, ICollection<T> entities, int? timeoutOverride = null, bool useContextConnection = true, ITableKeyProvider? tableKeyProvider = null, CancellationToken cancellationToken = default) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.MergeAsync(entities, tableKeyProvider, cancellationToken);
    }

    public static Task BulkInsertAsync<T>(this DbContext dbContext, IEnumerable<T> entities, int? timeoutOverride = null, bool useContextConnection = true, bool onConflictIgnore = false, CancellationToken cancellationToken = default) where T : class
    {
        var @operator = new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
        return @operator.InsertAsync(entities, onConflictIgnore, cancellationToken);
    }

    public static BulkEfOperator GetBulkOperator(this DbContext dbContext, int? timeoutOverride = null, bool useContextConnection = true)
    {
        return new BulkEfOperator(dbContext, timeoutOverride, useContextConnection);
    }
}
