namespace Domain.Entities;

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 50;
    public int MaxConcurrency { get; set; } = 8;

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(15);   // на одно сообщение
    public TimeSpan FinalizeTimeout { get; set; } = TimeSpan.FromSeconds(15);  // на всю финализацию

    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(2);      // когда пусто
    public TimeSpan ErrorBackoffMax { get; set; } = TimeSpan.FromSeconds(30);  // когда SQL/SB недоступны

    public int MaxAttempts { get; set; } = 8;
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    // Худший случай для пачки: волны по MaxConcurrency сообщений, каждая до PublishTimeout,
    // затем финализация. Аренда должна покрывать это с запасом 25%.
    public bool IsLeaseSufficient(out string error)
    {
        var waves = (int)Math.Ceiling((double)BatchSize / MaxConcurrency);
        var worstCase = TimeSpan.FromTicks(PublishTimeout.Ticks * waves) + FinalizeTimeout;
        var required = TimeSpan.FromTicks((long)(worstCase.Ticks * 1.25));

        error = $"LeaseDuration {LeaseDuration} < required {required} " +
                $"(waves={waves}, publishTimeout={PublishTimeout}, finalizeTimeout={FinalizeTimeout})";
        return LeaseDuration >= required;
    }
}
