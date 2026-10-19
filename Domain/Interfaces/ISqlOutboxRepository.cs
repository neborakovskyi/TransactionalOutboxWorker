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
    }
}
