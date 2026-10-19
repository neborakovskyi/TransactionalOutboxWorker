using Azure.Core.Pipeline;
using Domain.Entities;
using Domain.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Infastructure.Services;

public class OutboxMessageWorker(ISqlOutboxRepository repository,
        IOutboxPublisher publisher,
        IOutboxErrorClassifier classifier,
        IOutboxRetryPolicy retryPolicy,
        IOptions<OutboxOptions> options,
        WorkerIdentity workerId,                 // singleton: $"{MachineName}:{Guid}", уникален на процесс
        ILogger<OutboxMessageWorker> logger):  BackgroundService
{
    // ───────────────────────────── Главный цикл ─────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation(
            "Outbox worker started: worker={WorkerId} batch={BatchSize} concurrency={Concurrency} lease={Lease}",
            workerId, options.Value.BatchSize, options.Value.MaxConcurrency, options.Value.LeaseDuration);

        // Стартовый jitter: инстансы, поднятые одновременно (деплой, scale-out),
        // не начинают опрос БД синхронно.
        if (!await TryDelayAsync(WithJitter(options.Value.PollInterval), stoppingToken))
            return;

        var consecutiveErrors = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            TimeSpan pause;

            try
            {
                var result = await ProcessBatchAsync(stoppingToken);

                if (result.Claimed == 0)
                {
                    // Очередь пуста: спим, а не крутим цикл.
                    consecutiveErrors = 0;
                    pause = WithJitter(options.Value.PollInterval);
                }
                else if (result.LooksUnhealthy)
                {
                    // Ни одной успешной отправки, а были только временные ошибки (или не удалась
                    // финализация в SQL): похоже на сбой Service Bus или SQL. Притормаживаем сам цикл,
                    // иначе мы за секунды сожжём попытки всего бэклога, перебирая одну пачку за другой.
                    consecutiveErrors++;
                    pause = GetErrorDelay(consecutiveErrors);
                    logger.LogWarning(
                        "Outbox batch made no progress (retry={Retry}, finalizeOk={FinalizeOk}); " +
                        "backing off {Delay} (consecutive={Consecutive})",
                        result.Retried, result.FinalizeOk, pause, consecutiveErrors);
                }
                else
                {
                    consecutiveErrors = 0;
                    // Пачка была полной, значит бэклог есть: берём следующую сразу.
                    // Иначе короткая пауза. Tight loop здесь исключён: каждая итерация
                    // делает реальную работу (отправку), а не пустой запрос.
                    pause = result.Claimed >= options.Value.BatchSize
                        ? TimeSpan.Zero
                        : WithJitter(options.Value.PollInterval);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // штатная остановка: это не ошибка
            }
            catch (Exception ex)
            {
                // Сюда попадает недоступный SQL (claim) и прочие неожиданные ошибки.
                // Выходить из ExecuteAsync нельзя: по умолчанию необработанное исключение останавливает хост.
                consecutiveErrors++;
                pause = GetErrorDelay(consecutiveErrors);
                logger.LogError(ex,
                    "Outbox loop failed (consecutive={Consecutive}); retrying in {Delay}",
                    consecutiveErrors, pause);
            }

            if (pause > TimeSpan.Zero && !await TryDelayAsync(pause, stoppingToken))
                break;
        }

        logger.LogInformation("Outbox worker stopped: worker={WorkerId}", workerId);
    }

    // ───────────────────────────── Обработка одной пачки ─────────────────────────────

    private async Task<BatchResult> ProcessBatchAsync(CancellationToken stoppingToken)
    {
        // Время отсчитываем ДО claim: реальная аренда в БД истекает не раньше, чем
        // claimStartedAt + LeaseDuration, так что оценка консервативная.
        var claimStartedAt = Stopwatch.GetTimestamp();

        var batch = await repository.ClaimBatchAsync(
            workerId.Value, options.Value.BatchSize, options.Value.LeaseDuration, stoppingToken);

        if (batch.Count == 0)
            return BatchResult.Empty;

        // Все записи пачки делят один LeaseId (см. ClaimBatchAsync).
        var leaseId = batch[0].LeaseId
            ?? throw new InvalidOperationException("Claimed message has no LeaseId");

        using var scope = logger.BeginScope(new Dictionary<string, object>
        {
            ["WorkerId"] = workerId,
            ["LeaseId"] = leaseId
        });

        // Результаты собираем в потокобезопасную очередь: параллельные задачи пишут,
        // финализация читает после завершения всех.
        var outcomes = new ConcurrentQueue<MessageOutcome>();

        try
        {
            await Parallel.ForEachAsync(
                batch,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = options.Value.MaxConcurrency, // ограничение параллелизма
                    CancellationToken = stoppingToken
                },
                async (message, _) =>
                {
                    // PublishOneAsync не бросает исключений (кроме отмены при остановке),
                    // иначе Parallel.ForEachAsync прервал бы остальные задачи и мы потеряли бы
                    // уже полученные результаты.
                    var outcome = await PublishOneAsync(message, claimStartedAt, stoppingToken);
                    if (outcome is { } o)
                        outcomes.Enqueue(o);
                });
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Остановка приложения посреди пачки. НЕ выходим: сообщения, которые уже успели
            // уйти в Service Bus, надо пометить Published, иначе они будут отправлены повторно.
            logger.LogInformation("Shutdown requested during batch; finalizing completed messages");
        }

        // Финализация идёт с ОТДЕЛЬНЫМ токеном (таймаут, но не stoppingToken):
        // иначе при остановке мы бы потеряли результаты уже подтверждённых отправок.
        // Укладываемся в HostOptions.ShutdownTimeout, см. регистрацию ниже.
        using var finalizeCts = new CancellationTokenSource(options.Value.FinalizeTimeout);
        var result = await FinalizeAsync(leaseId, batch, outcomes, finalizeCts.Token);

        logger.LogInformation(
            "Outbox batch done: claimed={Claimed} published={Published} retry={Retry} " +
            "failed={Failed} released={Released} finalizeOk={FinalizeOk} elapsed={ElapsedMs}ms",
            result.Claimed, result.Published, result.Retried, result.Failed, result.Released,
            result.FinalizeOk, (long)Stopwatch.GetElapsedTime(claimStartedAt).TotalMilliseconds);

        return result;
    }

    // ───────────────────────────── Отправка одного сообщения ─────────────────────────────

    // Возвращает null, если попытка не состоялась или была прервана остановкой.
    // Такая запись будет возвращена в очередь через ReleaseAsync.
    private async Task<MessageOutcome?> PublishOneAsync(
        OutboxMessage message, long claimStartedAt, CancellationToken stoppingToken)
    {
        // 1) Poison-запись: Attempts растёт при claim, поэтому значение > MaxAttempts возможно только
        //    если предыдущие попытки не дошли до финализации (падение процесса, истечение аренды).
        //    Не отправляем и не ходим по кругу.
        if (message.Attempts > options.Value.MaxAttempts)
        {
            logger.LogError(
                "Outbox message {EventId} (id={Id}) exceeded max attempts ({Attempts}) without completing; marking Failed",
                message.EventId, message.Id, message.Attempts);
            return MessageOutcome.Failed(message,
                $"Exceeded max attempts ({message.Attempts}); previous attempts did not complete");
        }

        // 2) Бюджет аренды: если сообщение не успеет отправиться и финализироваться до конца аренды,
        //    не начинаем его. Иначе другой Worker заберёт запись, пока мы отправляем, и будет дубликат.
        var elapsed = Stopwatch.GetElapsedTime(claimStartedAt);
        if (elapsed + options.Value.PublishTimeout + options.Value.FinalizeTimeout > options.Value.LeaseDuration)
        {
            logger.LogWarning(
                "Skipping message {EventId} (id={Id}): lease budget exhausted (elapsed={Elapsed})",
                message.EventId, message.Id, elapsed);
            return null; // вернётся в очередь без потери попытки
        }

        // 3) Таймаут на одно сообщение. Linked-токен: срабатывает и от остановки, и от таймаута,
        //    и по типу причины мы отличаем их ниже.
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeoutCts.CancelAfter(options.Value.PublishTimeout);

        var sw = Stopwatch.StartNew();
        try
        {
            await publisher.PublishAsync(message, timeoutCts.Token);

            logger.LogDebug(
                "Published {EventId} (id={Id}, type={Type}, attempt={Attempt}) in {ElapsedMs}ms",
                message.EventId, message.Id, message.Type, message.Attempts, sw.ElapsedMilliseconds);

            return MessageOutcome.Published(message);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Остановка приложения, а не ошибка публикации. Исход неизвестен (брокер мог успеть принять
            // сообщение), поэтому не помечаем ни Failed, ни Retry: запись возвращается в очередь.
            // Дубликат в этом редком случае допустим по контракту at-least-once.
            return null;
        }
        catch (Exception ex)
        {
            // OperationCanceledException без отмены stoppingToken = сработал PublishTimeout.
            var kind = ex is OperationCanceledException
                ? FailureKind.Transient
                : classifier.Classify(ex);

            var error = Truncate(ex is OperationCanceledException
                ? $"Publish timed out after {options.Value.PublishTimeout}"
                : $"{ex.GetType().Name}: {ex.Message}");

            if (kind == FailureKind.Permanent)
            {
                logger.LogError(ex,
                    "Permanent publish failure {EventId} (id={Id}, type={Type}, attempt={Attempt}); marking Failed",
                    message.EventId, message.Id, message.Type, message.Attempts);
                return MessageOutcome.Failed(message, error);
            }

            if (message.Attempts >= options.Value.MaxAttempts)
            {
                logger.LogError(ex,
                    "Transient failures exhausted for {EventId} (id={Id}, type={Type}) after {Attempt} attempts; marking Failed",
                    message.EventId, message.Id, message.Type, message.Attempts);
                return MessageOutcome.Failed(message, $"Max attempts reached. Last error: {error}");
            }

            var delay = retryPolicy.GetDelay(message.Attempts);
            logger.LogWarning(ex,
                "Transient publish failure {EventId} (id={Id}, type={Type}, attempt={Attempt}/{Max}); retry in {Delay}",
                message.EventId, message.Id, message.Type, message.Attempts, options.Value.MaxAttempts, delay);
            return MessageOutcome.Retry(message, delay, error);
        }
    }

    // ───────────────────────────── Пакетная финализация ─────────────────────────────

    private async Task<BatchResult> FinalizeAsync(
        Guid leaseId,
        IReadOnlyList<OutboxMessage> batch,
        ConcurrentQueue<MessageOutcome> outcomes,
        CancellationToken ct)
    {
        var all = outcomes.ToArray();

        var published = all.Where(o => o.Kind == OutcomeKind.Published).Select(o => o.Message.Id).ToArray();
        var retries = all.Where(o => o.Kind == OutcomeKind.Retry)
                         .Select(o => new RetryRequest(o.Message.Id, o.RetryDelay, o.Error!)).ToArray();
        var failures = all.Where(o => o.Kind == OutcomeKind.Failed)
                          .Select(o => new FailRequest(o.Message.Id, o.Error!)).ToArray();

        // Всё, что не получило исхода (пропущено по бюджету аренды или прервано остановкой),
        // возвращаем в очередь.
        var decided = all.Select(o => o.Message.Id).ToHashSet();
        var released = batch.Where(m => !decided.Contains(m.Id)).Select(m => m.Id).ToArray();

        // По одному SQL-запросу на тип исхода, а не на запись.
        // Шаги независимы: сбой одного не мешает остальным.
        var ok = true;
        ok &= await RunStepAsync("MarkPublished", published.Length,
            c => repository.MarkPublishedAsync(published, leaseId, c), ct);
        ok &= await RunStepAsync("ScheduleRetry", retries.Length,
            c => repository.ScheduleRetryAsync(retries, leaseId, c), ct);
        ok &= await RunStepAsync("MarkFailed", failures.Length,
            c => repository.MarkFailedAsync(failures, leaseId, c), ct);
        ok &= await RunStepAsync("Release", released.Length,
            c => repository.ReleaseAsync(released, leaseId, c), ct);

        return new BatchResult(batch.Count, published.Length, retries.Length,
                               failures.Length, released.Length, ok);
    }

    // Небольшой retry вокруг SQL: сообщения уже ушли в Service Bus, потерять отметку об этом обидно.
    private async Task<bool> RunStepAsync(
        string step, int expected, Func<CancellationToken, Task<int>> action, CancellationToken ct)
    {
        if (expected == 0) return true;

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var affected = await action(ct);

                if (affected < expected)
                {
                    // Часть записей уже перехвачена другим Worker (аренда истекла).
                    // Fencing сработал: мы ничего не перезаписали. Но возможны дубликаты публикации.
                    logger.LogWarning(
                        "{Step}: updated {Affected}/{Expected} rows; lease lost for {Lost}. " +
                        "Possible duplicate publish (Consumer must be idempotent)",
                        step, affected, expected, expected - affected);
                }
                return true;
            }
            catch (Exception ex) when (attempt < 3 && !ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "{Step} failed (attempt {Attempt}/3); retrying", step, attempt);
                try { await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), ct); }
                catch (OperationCanceledException) { /* ct истёк: следующая итерация упадёт и уйдёт в catch ниже */ }
            }
            catch (Exception ex)
            {
                // Исчерпали попытки или истёк FinalizeTimeout. Не бросаем: записи останутся
                // Processing и вернутся в очередь после истечения аренды (возможен дубликат).
                logger.LogError(ex,
                    "{Step} failed for {Count} messages; rows will be reclaimed after lease expiry",
                    step, expected);
                return false;
            }
        }
    }

    // ───────────────────────────── Вспомогательное ─────────────────────────────

    private TimeSpan GetErrorDelay(int consecutiveErrors)
    {
        // 1s, 2s, 4s ... до ErrorBackoffMax, с jitter
        var exponent = Math.Min(consecutiveErrors - 1, 10);
        var raw = TimeSpan.FromSeconds(Math.Pow(2, exponent));
        var capped = raw > options.Value.ErrorBackoffMax ? options.Value.ErrorBackoffMax : raw;
        return WithJitter(capped);
    }

    private static TimeSpan WithJitter(TimeSpan delay) =>
        TimeSpan.FromMilliseconds(delay.TotalMilliseconds * (0.8 + Random.Shared.NextDouble() * 0.4));

    // false = отмена при остановке (штатный выход)
    private static async Task<bool> TryDelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try { await Task.Delay(delay, ct); return true; }
        catch (OperationCanceledException) { return false; }
    }

    private static string Truncate(string s) => s.Length <= 2000 ? s : s[..2000]; // LastError NVARCHAR(2000)

    // ───────────────────────────── Внутренние типы ─────────────────────────────

    private enum OutcomeKind { Published, Retry, Failed }

    private readonly record struct MessageOutcome(
        OutboxMessage Message, OutcomeKind Kind, TimeSpan RetryDelay = default, string? Error = null)
    {
        public static MessageOutcome Published(OutboxMessage m) => new(m, OutcomeKind.Published);
        public static MessageOutcome Retry(OutboxMessage m, TimeSpan delay, string error) => new(m, OutcomeKind.Retry, delay, error);
        public static MessageOutcome Failed(OutboxMessage m, string error) => new(m, OutcomeKind.Failed, Error: error);
    }

    private readonly record struct BatchResult(
        int Claimed, int Published, int Retried, int Failed, int Released, bool FinalizeOk)
    {
        public static readonly BatchResult Empty = new(0, 0, 0, 0, 0, true);

        // Нет ни одной успешной отправки и ни одного постоянного отказа (т.е. нет движения очереди),
        // при этом были временные ошибки, либо SQL не принял результаты.
        public bool LooksUnhealthy =>
            !FinalizeOk || (Published == 0 && Failed == 0 && Retried > 0);
    }
}

public sealed record WorkerIdentity(string Value);
