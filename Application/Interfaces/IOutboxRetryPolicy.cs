namespace Domain.Interfaces
{
    public interface IOutboxRetryPolicy
    {
        // attempt начинается с 1 (Attempts уже увеличен при claim)
        TimeSpan GetDelay(int attempt);
    }
}
