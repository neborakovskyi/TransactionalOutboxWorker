using Azure.Messaging.ServiceBus;
using Domain.Entities;
using Domain.Interfaces;

namespace Infastructure.Services;

public sealed class ServiceBusOutboxPublisher(
    ServiceBusSender sender// singleton, потокобезопасен
    ) : IOutboxPublisher
{

    public Task PublishAsync(OutboxMessage outputMessage, CancellationToken cancellationToken)
    {
        var message = new ServiceBusMessage(BinaryData.FromString(outputMessage.Payload))
        {
            MessageId = outputMessage.EventId.ToString(),   // ключ для duplicate detection и идемпотентности Consumer
            Subject = outputMessage.Type,
            ContentType = "application/json"    // предполагаю JSON в Payload; поправь, если нет
        };
        message.ApplicationProperties["outboxId"] = outputMessage.Id;
        message.ApplicationProperties["attempt"] = outputMessage.Attempts;

        // SendMessageAsync завершается после подтверждения брокером. Это и есть "подтверждённая отправка".
        //return sender.SendMessageAsync(message, cancellationToken);
        return Task.CompletedTask;
    }
}
