using Bogus;
using Microsoft.EntityFrameworkCore;
using NanoidDotNet;
using Npgsql;
using PgBulk.EFCore;

namespace PgBulk.Tests;

[TestClass]
public class TimeoutTests
{
    private static Faker<TestRow> Faker => new Faker<TestRow>().RuleFor(i => i.Id, f => f.IndexFaker)
        .RuleFor(i => i.Value1, f => f.Address.City())
        .RuleFor(i => i.Value2, f => f.Company.CompanyName())
        .RuleFor(i => i.Value3, f => f.PickRandom(null, f.Name.Suffix()))
        .RuleFor(i => i.Value4, f => f.PickRandom<TestEnum?>(null, TestEnum.Value1, TestEnum.Value2, TestEnum.Value3));

    private static MyContext CreateContext()
    {
        return EntityHelper.CreateContext(Nanoid.Generate(size: 8));
    }

    #region CommandTimeout on ExecuteCommand

    [TestMethod]
    public async Task CommandTimeout_TooShort_ThrowsOnSlowQuery()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(useContextConnection: false);
            @operator.CommandTimeout = 1;

            await using var connection = await @operator.CreateOpenedConnection();

            // pg_sleep(5) will take 5 seconds, but timeout is 1 second
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_sleep(5)";
            cmd.CommandTimeout = @operator.CommandTimeout;

            try
            {
                await cmd.ExecuteNonQueryAsync();
                Assert.Fail("Expected NpgsqlException due to timeout");
            }
            catch (NpgsqlException)
            {
                // Expected — timeout fired
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task CommandTimeout_Sufficient_Succeeds()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(useContextConnection: false);
            @operator.CommandTimeout = 10;

            await using var connection = await @operator.CreateOpenedConnection();

            // pg_sleep(1) takes 1 second, timeout is 10 seconds — should succeed
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_sleep(1)";
            cmd.CommandTimeout = @operator.CommandTimeout;
            await cmd.ExecuteNonQueryAsync();
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion

    #region CommandTimeout on connection string (binary importer)

    [TestMethod]
    public async Task ConnectionString_ContainsCommandTimeout()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(timeoutOverride: 42, useContextConnection: false);

            await using var connection = await @operator.CreateOpenedConnection();

            // The connection string should contain the command timeout
            var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            Assert.AreEqual(42, builder.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task ConnectionString_DefaultTimeout_IsZero()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(useContextConnection: false);

            await using var connection = await @operator.CreateOpenedConnection();

            var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            Assert.AreEqual(0, builder.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion

    #region CancellationToken

    [TestMethod]
    public async Task CancellationToken_CancelsInsert()
    {
        await using var myContext = CreateContext();

        try
        {
            using var cts = new CancellationTokenSource();
            var entities = Faker.Generate(100_000);

            // Cancel almost immediately
            cts.CancelAfter(TimeSpan.FromMilliseconds(50));

            try
            {
                await myContext.BulkInsertAsync(entities, cancellationToken: cts.Token);
                Assert.Fail("Expected OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task CancellationToken_CancelsMerge()
    {
        await using var myContext = CreateContext();

        try
        {
            using var cts = new CancellationTokenSource();
            var entities = Faker.Generate(100_000);

            cts.CancelAfter(TimeSpan.FromMilliseconds(50));

            try
            {
                await myContext.BulkMergeAsync(entities, cancellationToken: cts.Token);
                Assert.Fail("Expected OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task CancellationToken_CancelsSync()
    {
        await using var myContext = CreateContext();

        try
        {
            using var cts = new CancellationTokenSource();
            var entities = Faker.Generate(100_000);

            cts.CancelAfter(TimeSpan.FromMilliseconds(50));

            try
            {
                await myContext.BulkSyncAsync(entities, cancellationToken: cts.Token);
                Assert.Fail("Expected OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task CancellationToken_AlreadyCancelled_Throws()
    {
        await using var myContext = CreateContext();

        try
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var entities = Faker.Generate(10);

            try
            {
                await myContext.BulkInsertAsync(entities, cancellationToken: cts.Token);
                Assert.Fail("Expected OperationCanceledException");
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion

    #region Operations succeed with default timeout (infinite)

    [TestMethod]
    public async Task Insert_SucceedsWithDefaultTimeout()
    {
        await using var myContext = CreateContext();

        try
        {
            var entities = Faker.Generate(1000);
            await myContext.BulkInsertAsync(entities);

            var count = await myContext.TestRows.CountAsync();
            Assert.AreEqual(1000, count);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task Merge_SucceedsWithDefaultTimeout()
    {
        await using var myContext = CreateContext();

        try
        {
            var entities = Faker.Generate(1000);
            await myContext.BulkMergeAsync(entities);

            var count = await myContext.TestRows.CountAsync();
            Assert.AreEqual(1000, count);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task Sync_SucceedsWithDefaultTimeout()
    {
        await using var myContext = CreateContext();

        try
        {
            var entities = Faker.Generate(1000);
            await myContext.BulkSyncAsync(entities);

            var count = await myContext.TestRows.CountAsync();
            Assert.AreEqual(1000, count);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion

    #region BulkEfOperator timeout propagation

    [TestMethod]
    public async Task BulkEfOperator_TimeoutOverride_IsApplied()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(timeoutOverride: 123);
            Assert.AreEqual(123, @operator.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task BulkEfOperator_DefaultTimeout_IsZero()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator();
            Assert.AreEqual(0, @operator.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task BulkEfOperator_TimeoutTooShort_SlowQueryFails()
    {
        await using var myContext = CreateContext();

        try
        {
            var @operator = myContext.GetBulkOperator(timeoutOverride: 1, useContextConnection: false);

            await using var connection = await @operator.CreateOpenedConnection();

            // Execute a slow query that should timeout
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT pg_sleep(5)";
            cmd.CommandTimeout = @operator.CommandTimeout;

            try
            {
                await cmd.ExecuteNonQueryAsync();
                Assert.Fail("Expected NpgsqlException due to timeout");
            }
            catch (NpgsqlException)
            {
                // Expected — timeout fired
            }
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion

    #region Manual BulkOperator timeout

    [TestMethod]
    public async Task ManualOperator_CommandTimeout_IsApplied()
    {
        await using var myContext = CreateContext();

        try
        {
            var provider = new ManualTableInformationProvider()
                .AddTableMapping<TestRow>("TestRows", c => c.Automap());

            var @operator = new ManualBulkOperator(myContext.Database.GetConnectionString(), provider);
            @operator.CommandTimeout = 99;

            Assert.AreEqual(99, @operator.CommandTimeout);

            await using var connection = await @operator.CreateOpenedConnection();

            var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            Assert.AreEqual(99, builder.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    [TestMethod]
    public async Task ManualOperator_DefaultTimeout_IsZero()
    {
        await using var myContext = CreateContext();

        try
        {
            var provider = new ManualTableInformationProvider()
                .AddTableMapping<TestRow>("TestRows", c => c.Automap());

            var @operator = new ManualBulkOperator(myContext.Database.GetConnectionString(), provider);

            Assert.AreEqual(0, @operator.CommandTimeout);

            await using var connection = await @operator.CreateOpenedConnection();

            var builder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            Assert.AreEqual(0, builder.CommandTimeout);
        }
        finally
        {
            await myContext.Database.EnsureDeletedAsync();
        }
    }

    #endregion
}
