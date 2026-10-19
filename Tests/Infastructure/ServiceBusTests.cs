using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;

namespace Tests.Infastructure;

public class ServiceBusTests
{
    [Fact]
    public async Task ServiceBusTest_Ping()
    {
        await using var client = new ServiceBusClient("Endpoint=sb://my-worker.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=JjXCLsoiyat0x7oqiY3qZyWo+fWSflYep+ASbDlgcEw=");
        await using var sender = client.CreateSender("outbox-events");
        //uncomment it out if need
        //await sender.SendMessageAsync(new ServiceBusMessage("ping"));
        Console.WriteLine("sent");
    }
}
