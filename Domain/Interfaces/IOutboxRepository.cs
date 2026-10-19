using Domain.Entities;
using System;
using System.Collections.Generic;
using System.Text;

namespace Domain.Interfaces;

public interface IOutboxRepository
{
    Task<OutboxMessage?> GetByKeyAsync(Guid eventId, CancellationToken cancellationToken = default);
}
