using Npgsql;
using PgBulk.Abstractions;

namespace PgBulk;

public sealed class NpgsqlBinaryImporter<T> : IDisposable, IAsyncDisposable
{
    private readonly NpgsqlBinaryImporter _binaryImporter;

    private readonly IReadOnlyList<ITableColumnInformation> _columns;

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
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            ulong inserted = 0;

            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await WriteRowAsync(entity, cancellationToken);
                inserted++;
            }

            return inserted;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask WriteToBinaryImporter(T entity, CancellationToken cancellationToken = default)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await WriteRowAsync(entity, cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async ValueTask WriteRowAsync(T entity, CancellationToken cancellationToken)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        await _binaryImporter.StartRowAsync(cancellationToken);

        for (var i = 0; i < _columns.Count; i++)
        {
            await _binaryImporter.WriteAsync(_columns[i].GetValue(entity), cancellationToken);
        }
    }

    public async ValueTask WriteValuesAsync(IEnumerable<object?> values, CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);

        try
        {
            await _binaryImporter.StartRowAsync(cancellationToken);

            foreach (var value in values)
                await _binaryImporter.WriteAsync(value, cancellationToken);
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
