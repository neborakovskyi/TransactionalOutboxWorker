namespace Domain.Entities;

public sealed class ServiceBusSettings
{
    public string? ConnectionString { get; set; }          // для локальной разработки
    public string? FullyQualifiedNamespace { get; set; }   // для Azure (Managed Identity)
    public string EntityName { get; set; } = null!;        // имя очереди или топика
}