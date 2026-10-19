using Azure.Messaging.ServiceBus;
using Domain.Entities;
using Domain.Interfaces;

namespace Infastructure.Services;

public sealed class ServiceBusErrorClassifier : IOutboxErrorClassifier
{
    public FailureKind Classify(Exception ex) => ex switch
    {
        // Проблема в самом сообщении: ретраи бессмысленны
        ServiceBusException { Reason: ServiceBusFailureReason.MessageSizeExceeded } => FailureKind.Permanent,
        ArgumentException or FormatException or System.Text.Json.JsonException => FailureKind.Permanent,

        // Всё остальное, включая неизвестные исключения и "entity not found / unauthorized"
        // (это конфигурация, а не вина сообщения), считаем временным.
        // Ограничивает их MaxAttempts, после лимита запись уйдёт в Failed.
        _ => FailureKind.Transient
    };
}