namespace Domain.Entities;

public readonly record struct RetryRequest(long Id, TimeSpan Delay, string Error);
