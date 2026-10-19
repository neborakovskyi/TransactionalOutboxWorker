using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Domain.Entities;
using Domain.Interfaces;
using Infastructure.Persistense;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Tests.Infastructure.Persistense;

public class SqlOutboxRepositoryTests
{
    // The tests expect a real SQL Server connection string in the environment variable "TEST_DB_CONNECTION".
    // Run these tests against a disposable/test database. They will create the dbo.OutboxMessages table if missing
    // and will delete any rows they insert (identified by EventId).
    private static string GetConnectionString()
    {
        var cs = "Server=(localdb)\\mssqllocaldb;Database=Worker;Trusted_Connection=True;MultipleActiveResultSets=true";
        if (string.IsNullOrWhiteSpace(cs))
            throw new InvalidOperationException("Set TEST_DB_CONNECTION environment variable to a test SQL Server connection string.");
        return cs;
    }

    private class TestSqlConnectionFactory : ISqlConnectionFactory
    {
        private readonly string _connectionString;
        public TestSqlConnectionFactory(string connectionString) => _connectionString = connectionString;

        public async Task<System.Data.Common.DbConnection> OpenAsync(CancellationToken cancellationToken)
        {
            var c = new SqlConnection(_connectionString);
            await c.OpenAsync(cancellationToken);
            return c;
        }
    }

    [Fact]
    public async Task ClaimBatchAsync_Integration_ReturnsRows_AndNormalizesUtcKinds()
    {
        var cs = GetConnectionString();
        var factory = new TestSqlConnectionFactory(cs);
        var logger = new Mock<ILogger<SqlOutboxRepository>>();

        var repo = new SqlOutboxRepository(factory, logger.Object);

        // Ensure table exists (minimal schema required by the repository)
        //await EnsureTableExists(cs);

        // Insert test rows identified by EventId so we can clean them up afterwards
        var e1 = Guid.NewGuid();
        var e2 = Guid.NewGuid();

        var insertSql = @"
            INSERT INTO dbo.OutboxMessages
                (EventId, [Type], Payload, CreatedAtUtc, PublishedAtUtc, NextAttemptAtUtc, LockedUntilUtc, LockOwner, Attempts, [Status], LastError)
            VALUES
                (@EventId, @Type, @Payload, @CreatedAtUtc, @PublishedAtUtc, @NextAttemptAtUtc, @LockedUntilUtc, @LockOwner, @Attempts, @Status, @LastError);
            ";

        await using (var conn = new SqlConnection(cs))
        {
            await conn.OpenAsync();
            // Row 2 (Id higher) intentionally inserted first to test sorting inside the repo
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = insertSql;
                cmd.Parameters.Add(new SqlParameter("@EventId", e2));
                cmd.Parameters.Add(new SqlParameter("@Type", "T"));
                cmd.Parameters.Add(new SqlParameter("@Payload", "P2"));
                cmd.Parameters.Add(new SqlParameter("@CreatedAtUtc", DateTime.UtcNow.AddMinutes(-5)));
                cmd.Parameters.Add(new SqlParameter("@PublishedAtUtc", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@NextAttemptAtUtc", DateTime.UtcNow.AddMinutes(-1))); // ready
                cmd.Parameters.Add(new SqlParameter("@LockedUntilUtc", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@LockOwner", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Attempts", 2));
                cmd.Parameters.Add(new SqlParameter("@Status", "Pending"));
                cmd.Parameters.Add(new SqlParameter("@LastError", DBNull.Value));
                await cmd.ExecuteNonQueryAsync();
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = insertSql;
                cmd.Parameters.Add(new SqlParameter("@EventId", e1));
                cmd.Parameters.Add(new SqlParameter("@Type", "T"));
                cmd.Parameters.Add(new SqlParameter("@Payload", "P1"));
                cmd.Parameters.Add(new SqlParameter("@CreatedAtUtc", DateTime.UtcNow.AddMinutes(-10)));
                cmd.Parameters.Add(new SqlParameter("@PublishedAtUtc", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@NextAttemptAtUtc", DateTime.UtcNow.AddMinutes(-1)));
                cmd.Parameters.Add(new SqlParameter("@LockedUntilUtc", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@LockOwner", DBNull.Value));
                cmd.Parameters.Add(new SqlParameter("@Attempts", 1));
                cmd.Parameters.Add(new SqlParameter("@Status", "Pending"));
                cmd.Parameters.Add(new SqlParameter("@LastError", DBNull.Value));
                await cmd.ExecuteNonQueryAsync();
            }
        }

        try
        {
            var result = await repo.ClaimBatchAsync("worker-integration", 10, TimeSpan.FromSeconds(30), CancellationToken.None);

            Assert.NotNull(result);
            Assert.NotEmpty(result);

            // Sorted by Id ascending (Id assigned by DB). We cannot predict absolute Ids, but order must be ascending.
            for (int i = 1; i < result.Count; i++)
            {
                Assert.True(result[i - 1].Id < result[i].Id);
            }

            // DateTime kinds normalized to Utc
            foreach (var r in result)
            {
                Assert.Equal(DateTimeKind.Utc, r.CreatedAtUtc.Kind);
                if (r.LockedUntilUtc.HasValue)
                    Assert.Equal(DateTimeKind.Utc, r.LockedUntilUtc.Value.Kind);
            }

            // Logger should have received an Information call because rows were claimed
            logger.Verify(l => l.Log(
                    Microsoft.Extensions.Logging.LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Outbox claim")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce());
        }
        finally
        {
            // Clean up inserted rows
            await using var cleanupConn = new SqlConnection(cs);
            await cleanupConn.OpenAsync();
            await using var cleanupCmd = cleanupConn.CreateCommand();
            cleanupCmd.CommandText = "DELETE FROM dbo.OutboxMessages WHERE EventId IN (@e1, @e2)";
            cleanupCmd.Parameters.Add(new SqlParameter("@e1", e1));
            cleanupCmd.Parameters.Add(new SqlParameter("@e2", e2));
            await cleanupCmd.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task ClaimBatchAsync_Integration_NoRows_LogsDebug()
    {
        var cs = GetConnectionString();
        var factory = new TestSqlConnectionFactory(cs);
        var logger = new Mock<ILogger<SqlOutboxRepository>>();
        var repo = new SqlOutboxRepository(factory, logger.Object);

        //await EnsureTableExists(cs);

        // Ensure there are no ready rows: delete any Pending/Processing rows that are ready to be picked
        await using (var conn = new SqlConnection(cs))
        {
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                DELETE FROM dbo.OutboxMessages
                WHERE ( [Status] = N'Pending' AND NextAttemptAtUtc <= SYSUTCDATETIME() )
                   OR ( [Status] = N'Processing' AND LockedUntilUtc <= SYSUTCDATETIME() )
                ";
            await cmd.ExecuteNonQueryAsync();
        }

        var result = await repo.ClaimBatchAsync("worker-empty", 5, TimeSpan.FromSeconds(10), CancellationToken.None);

        Assert.NotNull(result);
        Assert.Empty(result);

        logger.Verify(l => l.Log(
                Microsoft.Extensions.Logging.LogLevel.Debug,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Outbox claim")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception, string>>()
            ), Times.AtLeastOnce);
    }

    [Fact]
    public async Task ClaimBatchAsync_ThrowsOnInvalidArguments()
    {
        var cs = GetConnectionString();
        var factory = new TestSqlConnectionFactory(cs);
        var logger = new Mock<ILogger<SqlOutboxRepository>>();
        var repo = new SqlOutboxRepository(factory, logger.Object);

        await Assert.ThrowsAsync<ArgumentException>(() => repo.ClaimBatchAsync("", 1, TimeSpan.FromSeconds(1), CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => repo.ClaimBatchAsync("w", 0, TimeSpan.FromSeconds(1), CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => repo.ClaimBatchAsync("w", 1, TimeSpan.Zero, CancellationToken.None));
    }

    private static async Task EnsureTableExists(string connectionString)
    {
        var createSql = @"
            IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'OutboxMessages' AND s.name = 'dbo')
            BEGIN
                CREATE TABLE dbo.OutboxMessages
                (
                    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    EventId UNIQUEIDENTIFIER NOT NULL,
                    [Type] NVARCHAR(200) NOT NULL,
                    Payload NVARCHAR(MAX) NOT NULL,
                    CreatedAtUtc DATETIME2 NOT NULL,
                    PublishedAtUtc DATETIME2 NULL,
                    NextAttemptAtUtc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    LockedUntilUtc DATETIME2 NULL,
                    LockOwner NVARCHAR(200) NULL,
                    Attempts INT NOT NULL DEFAULT 0,
                    [Status] NVARCHAR(50) NOT NULL DEFAULT N'Pending',
                    LastError NVARCHAR(MAX) NULL
                );
            END
            ";
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = createSql;
        await cmd.ExecuteNonQueryAsync();
    }
}