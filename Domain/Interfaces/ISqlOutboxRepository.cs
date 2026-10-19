using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Text;

namespace Domain.Interfaces
{
    public interface ISqlOutboxRepository
    {
        Task<IReadOnlyList<OutboxMessage>> ClaimBatchAsync(
            string workerId,
            int batchSize,
            TimeSpan leaseDuration,
            CancellationToken cancellationToken);

        // Все методы ниже: WHERE LeaseId = @leaseId AND Status = 'Processing' AND Id IN (...).
        // Возвращают число реально обновлённых строк. Меньше ожидаемого значит, что аренда потеряна.
        Task<int> MarkPublishedAsync(IReadOnlyCollection<long> ids, Guid leaseId, CancellationToken ct);
        Task<int> ScheduleRetryAsync(IReadOnlyCollection<RetryRequest> items, Guid leaseId, CancellationToken ct);
        Task<int> MarkFailedAsync(IReadOnlyCollection<FailRequest> items, Guid leaseId, CancellationToken ct);

        // Новый метод: вернуть в очередь записи, которые мы взяли, но так и не попытались отправить
        // (остановка приложения, нехватка времени аренды). Status='Pending', NextAttemptAtUtc=now,
        // Attempts=Attempts-1 (попытки не было), аренда снимается.
        Task<int> ReleaseAsync(IReadOnlyCollection<long> ids, Guid leaseId, CancellationToken ct);

    }

}
