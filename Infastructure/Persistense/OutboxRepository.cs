using Domain.Entities;
using Domain.Interfaces;
using Microsoft.EntityFrameworkCore;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infastructure.Persistense;

public class OutboxRepository(AppDbContext db) : IOutboxRepository
{
    public async Task<OutboxMessage?> GetByKeyAsync(Guid eventId, CancellationToken cancellationToken = default) => 
        await db.OutboxMessages.FirstOrDefaultAsync(w => w.EventId == eventId, cancellationToken);
}
