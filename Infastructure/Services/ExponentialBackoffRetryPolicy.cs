using Domain.Entities;
using Domain.Interfaces;
using Microsoft.Extensions.Options;

namespace Infastructure.Services;

public sealed class ExponentialBackoffRetryPolicy(IOptions<OutboxOptions> options) : IOutboxRetryPolicy
{
    public TimeSpan GetDelay(int attempt)
    {
        // base * 2^(attempt-1), с ограничением показателя, чтобы не получить переполнение
        var exponent = Math.Min(Math.Max(attempt - 1, 0), 20);
        var raw = TimeSpan.FromTicks((long)(options.Value.RetryBaseDelay.Ticks * Math.Pow(2, exponent)));
        var capped = raw > options.Value.RetryMaxDelay ? options.Value.RetryMaxDelay : raw;

        // "Equal jitter": половина фиксирована, половина случайна. Записи, упавшие одновременно,
        // не вернутся одной волной и не придавят Service Bus повторно.
        var half = capped.Ticks / 2;
        return TimeSpan.FromTicks(half + (long)(half * Random.Shared.NextDouble()));
    }
}
