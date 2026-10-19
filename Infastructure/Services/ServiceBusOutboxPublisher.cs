using Azure.Messaging.ServiceBus;
using Domain.Entities;
using Domain.Interfaces;

namespace Infastructure.Services;

public sealed class ServiceBusOutboxPublisher(
    ServiceBusSender sender// singleton, потокобезопасен
    ) : IOutboxPublisher
{

    public Task PublishAsync(OutboxMessage m, CancellationToken cancellationToken)
    {
        var message = new ServiceBusMessage(BinaryData.FromString(m.Payload))
        {
            MessageId = m.EventId.ToString(),   // ключ для duplicate detection и идемпотентности Consumer
            Subject = m.Type,
            ContentType = "application/json"    // предполагаю JSON в Payload; поправь, если нет
        };
        message.ApplicationProperties["outboxId"] = m.Id;
        message.ApplicationProperties["attempt"] = m.Attempts;

        // SendMessageAsync завершается после подтверждения брокером. Это и есть "подтверждённая отправка".
        return sender.SendMessageAsync(message, cancellationToken);
    }
}
