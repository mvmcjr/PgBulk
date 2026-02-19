using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

public sealed class NpgsqlBinaryImporter<T> : IDisposable, IAsyncDisposable
{
    private readonly NpgsqlBinaryImporter _binaryImporter;

    private readonly ICollection<ITableColumnInformation> _columns;

    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public NpgsqlBinaryImporter(NpgsqlBinaryImporter binaryImporter, IEnumerable<ITableColumnInformation> columns)
    {
        _binaryImporter = binaryImporter;
        _columns = columns.ToList();
    }

    public ValueTask DisposeAsync()
    {
        return _binaryImporter.DisposeAsync();
    }

    public void Dispose()
    {
        _binaryImporter.Dispose();
    }

    public async ValueTask<ulong> WriteToBinaryImporter(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ulong inserted = 0;

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await WriteToBinaryImporter(entity, cancellationToken);
            inserted++;
        }

        return inserted;
    }

    public ValueTask WriteToBinaryImporter(T entity, CancellationToken cancellationToken = default)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        return WriteToBinaryImporter(_columns.Select(c => c.GetValue(entity)), cancellationToken);
    }

    public async ValueTask WriteToBinaryImporter(IEnumerable<object?> values, CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await _binaryImporter.StartRowAsync(cancellationToken);

            foreach (var value in values) await _binaryImporter.WriteAsync(value, cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public ValueTask<ulong> CompleteAsync(CancellationToken cancellationToken = default)
    {
        return _binaryImporter.CompleteAsync(cancellationToken);
    }
}