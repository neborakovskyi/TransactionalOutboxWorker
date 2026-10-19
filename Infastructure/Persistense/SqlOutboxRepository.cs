using Domain.Entities;
using Domain.Interfaces;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Dapper;

namespace Infastructure.Persistense;

public class SqlOutboxRepository(ISqlConnectionFactory connections, 
    ILogger<SqlOutboxRepository> logger) : ISqlOutboxRepository
{
    // ВЕСЬ захват — ОДИН SQL-оператор. Это и есть источник атомарности:
    // SQL Server выполняет единичный DML-оператор как неявную транзакцию.
    private const string ClaimSql = """
    ;WITH candidates AS
    (
        -- TOP + ORDER BY живут в CTE, потому что в самом UPDATE ORDER BY недоступен.
        SELECT TOP (@BatchSize) *
        FROM dbo.OutboxMessages
             WITH (UPDLOCK,   -- сразу update-lock: без shared→update конверсии и deadlock'ов
                   READPAST,  -- строки, залоченные другими Worker, пропускаем, а не ждём
                   ROWLOCK)   -- не даём эскалироваться до блокировки таблицы
        WHERE
            (
                -- Случай 1: новая запись или запись после backoff, готовая к повтору
                [Status] = N'Pending'
                AND NextAttemptAtUtc <= SYSUTCDATETIME()
            )
            OR
            (
                -- Случай 2: прежний Worker упал или завис, аренда истекла
                [Status] = N'Processing'
                AND LockedUntilUtc <= SYSUTCDATETIME()
            )
        ORDER BY Id  -- приблизительный FIFO (строгий порядок не гарантируется)
    )
    UPDATE candidates
    SET [Status]       = N'Processing',
        LockOwner      = @WorkerId,
        LeaseId        = @LeaseId,
        -- аренда считается по часам SQL Server, а не приложения (нет clock skew между инстансами)
        LockedUntilUtc = DATEADD(SECOND, @LeaseSeconds, SYSUTCDATETIME()),
        -- Attempts растёт при захвате: poison-запись, убивающая процесс, тоже исчерпает лимит
        Attempts       = Attempts + 1
    -- OUTPUT возвращает ровно те строки, которые мы только что захватили,
    -- в том же round-trip: отдельного SELECT не нужно.
    OUTPUT inserted.Id, inserted.EventId, inserted.[Type], inserted.Payload,
           inserted.CreatedAtUtc, inserted.PublishedAtUtc, inserted.NextAttemptAtUtc,
           inserted.LockedUntilUtc, inserted.LockOwner, inserted.Attempts,
           inserted.[Status], inserted.LastError, inserteed.LeaseId;
    """;

    public async Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(string workerId, int batchSize, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        // Защита от неверной конфигурации: нулевая или отрицательная аренда
        // сделала бы записи сразу «просроченными» и захватываемыми повторно.
        ArgumentException.ThrowIfNullOrWhiteSpace(workerId);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(batchSize, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(leaseDuration, TimeSpan.Zero);

        var sw = Stopwatch.StartNew();

        // Соединение берём из пула; отдаётся обратно в пул через await using.
        await using var connection = await connections.OpenAsync(cancellationToken);

        // Явная транзакция НЕ нужна: оператор один, он сам атомарен.
        // Если соединение оборвётся ДО коммита, SQL Server откатит всё,
        // и ни одна запись не окажется захваченной.
        var command = new CommandDefinition(
            ClaimSql,
            new
            {
                BatchSize = batchSize,
                WorkerId = workerId,
                LeaseId = Guid.NewGuid(),// один Guid на всю пачку; сигнатура метода не меняется
                LeaseSeconds = (int)Math.Ceiling(leaseDuration.TotalSeconds)
            },
            cancellationToken: cancellationToken); // токен доходит до SqlCommand

        var rows = (await connection.QueryAsync<OutboxMessage>(command)).ToList();

        // Порядок строк в OUTPUT не гарантирован, поэтому упорядочиваем на клиенте.
        rows.Sort((a, b) => a.Id.CompareTo(b.Id));

        // Dapper отдаёт DateTime с Kind=Unspecified; фиксируем Utc,
        // чтобы дальше никто не интерпретировал значения как локальное время.
        foreach (var m in rows)
        {
            m.CreatedAtUtc = DateTime.SpecifyKind(m.CreatedAtUtc, DateTimeKind.Utc);
            if (m.LockedUntilUtc is { } datetime) // .HasValue()
                m.LockedUntilUtc = DateTime.SpecifyKind(datetime, DateTimeKind.Utc);
        }

        // Наблюдаемость: по этим логам видно и задержку в очереди, и повторные захваты.
        if (rows.Count > 0)
        {
            var oldestAge = DateTime.UtcNow - rows[0].CreatedAtUtc;
            var reclaimed = rows.Count(m => m.Attempts > 1); // уже пытались: ретрай или истёкшая аренда

            logger.LogInformation(
                $"Outbox claim: worker={workerId} claimed={rows.Count} reclaimed={reclaimed} " +
                $"oldestAge={(long)oldestAge.TotalMilliseconds}ms elapsed={sw.ElapsedMilliseconds}ms");
        }
        else
        {
            logger.LogDebug($"Outbox claim: worker={workerId} nothing to claim");
        }

        return rows;
    }

    public async Task<int> MarkFailedAsync(IReadOnlyCollection<FailRequest> items, Guid leaseId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public async Task<int> MarkPublishedAsync(IReadOnlyCollection<long> ids, Guid leaseId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public async Task<int> ReleaseAsync(IReadOnlyCollection<long> ids, Guid leaseId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }

    public async Task<int> ScheduleRetryAsync(IReadOnlyCollection<RetryRequest> items, Guid leaseId, CancellationToken ct)
    {
        throw new NotImplementedException();
    }
}

