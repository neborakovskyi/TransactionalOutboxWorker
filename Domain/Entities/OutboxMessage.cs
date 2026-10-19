using System;
using System.Collections.Generic;
using System.Text;

namespace Domain.Entities;

public class OutboxMessage
{
    public long Id { get; set; }
    public Guid EventId { get; set; }
    public string Type { get; set; } = null!;
    public string Payload { get; set; } = null!;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? PublishedAtUtc { get; set; }
    public DateTime? LockedUntilUtc { get; set; }
    public string? LockOwner { get; set; }
    public int Attempts { get; set; }
    public string Status { get; set; } = "Pending";
    public string? LastError { get; set; }
    public Guid? LeaseId { get; set; }

}
