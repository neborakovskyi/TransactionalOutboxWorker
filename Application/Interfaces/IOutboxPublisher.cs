using Domain.Entities;

namespace Domain.Interfaces
{
    public interface IOutboxPublisher
    {
        // Бросает исключение при неудаче. Классификация ошибок вынесена отдельно.
        Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken);
    }
}
