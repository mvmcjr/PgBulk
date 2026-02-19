using System.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PgBulk.EFCore;

public class BulkEfOperator : BulkOperator
{
    public BulkEfOperator(DbContext dbContext, int? timeoutOverride = null, bool useContextConnection = true) : base(dbContext.Database.GetConnectionString(), new EntityTableInformationProvider(dbContext))
    {
        DbContext = dbContext;
        DisposeConnection = false;
        UseContextConnection = useContextConnection;
        CommandTimeout = timeoutOverride ?? 0;

        var serviceProvider = dbContext.GetInfrastructure();
        Logger = serviceProvider.GetService<ILogger<BulkEfOperator>>();
    }

    private ILogger<BulkEfOperator>? Logger { get; }

    private DbContext DbContext { get; }

    private bool UseContextConnection { get; }

    public override async Task<NpgsqlConnection> CreateOpenedConnection(CancellationToken cancellationToken = default)
    {
        if (!UseContextConnection)
            return await base.CreateOpenedConnection(cancellationToken);

        if (DbContext.Database.GetDbConnection() is not NpgsqlConnection npgsqlConnection) throw new InvalidOperationException("Connection is not NpgsqlConnection");

        if (npgsqlConnection.State == ConnectionState.Closed)
            await npgsqlConnection.OpenAsync(cancellationToken);

        return npgsqlConnection;
    }

    public override void LogBeforeCommand(NpgsqlCommand npgsqlCommand)
    {
        Logger?.LogInformation("Executing command {@Command}", npgsqlCommand.CommandText);
    }

    public override void LogAfterCommand(NpgsqlCommand npgsqlCommand, TimeSpan elapsed)
    {
        Logger?.LogInformation("Executed DbCommand ({ElapsedMilliseconds}ms) {@Command}", elapsed.TotalMilliseconds, npgsqlCommand.CommandText);
    }
}