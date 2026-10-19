
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Domain.Entities;
using Domain.Interfaces;
using Infastructure.Persistense;
using Infastructure.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Outbox.Worker.Persistence;

namespace Api;

public class Program
{
    private const string dapper = "Dapper";

    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.
        builder.Services.AddSwaggerGen();
        builder.Services
            .AddOptions<OutboxOptions>()
            .Bind(builder.Configuration.GetSection("Outbox"))
            .Validate(o => o.IsLeaseSufficient(out _), "Outbox: LeaseDuration is too short for BatchSize/MaxConcurrency/timeouts")
            .ValidateOnStart(); // приложение не стартует с заведомо неверной конфигурацией
        
        builder.Services
            .AddOptions<ServiceBusSettings>()
            .Bind(builder.Configuration.GetSection("ServiceBus"))
            .Validate(s => !string.IsNullOrWhiteSpace(s.EntityName)
                           && (!string.IsNullOrWhiteSpace(s.ConnectionString)
                               || !string.IsNullOrWhiteSpace(s.FullyQualifiedNamespace)),
                      "ServiceBus: EntityName and either ConnectionString or FullyQualifiedNamespace are required")
            .ValidateOnStart();

        // Client: один на приложение, потокобезопасный, держит AMQP-соединение.
        builder.Services.AddSingleton(sp =>
        {
            var settings = sp.GetRequiredService<IOptions<ServiceBusSettings>>().Value;

            var clientOptions = new ServiceBusClientOptions
            {
                // Собственные ретраи SDK выполняются внутри SendMessageAsync, поверх них работает
                // ретрай Outbox. Оставляем SDK немного, иначе PublishTimeout (15 с) будет обрывать
                // отправку посреди его внутренних повторов.
                RetryOptions = new ServiceBusRetryOptions
                {
                    Mode = ServiceBusRetryMode.Exponential,
                    MaxRetries = 2,
                    Delay = TimeSpan.FromMilliseconds(500),
                    MaxDelay = TimeSpan.FromSeconds(3),
                    TryTimeout = TimeSpan.FromSeconds(10)
                }
                // Если исходящий порт 5671 закрыт (корпоративная сеть):
                // TransportType = ServiceBusTransportType.AmqpWebSockets
            };

            return !string.IsNullOrWhiteSpace(settings.ConnectionString)
                ? new ServiceBusClient(settings.ConnectionString, clientOptions)
                // Managed Identity / az login / VS credentials; нужна роль "Azure Service Bus Data Sender"
                : new ServiceBusClient(settings.FullyQualifiedNamespace, new DefaultAzureCredential(), clientOptions);
        });

        // Sender: создаётся из client, тоже singleton и потокобезопасен.
        builder.Services.AddSingleton(sp =>
        {
            var settings = sp.GetRequiredService<IOptions<ServiceBusSettings>>().Value;
            return sp.GetRequiredService<ServiceBusClient>().CreateSender(settings.EntityName);
        });

        builder.Services.AddSingleton(new WorkerIdentity($"{Environment.MachineName}:{Guid.NewGuid():N}"));
        builder.Services.AddSingleton<IOutboxRetryPolicy, ExponentialBackoffRetryPolicy>();
        builder.Services.AddSingleton<IOutboxErrorClassifier, ServiceBusErrorClassifier>();
        builder.Services.AddSingleton<IOutboxPublisher, ServiceBusOutboxPublisher>();

        // Остановка: отмена прерывает отправки сразу, но финализация может занять до FinalizeTimeout.
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(45));
        builder.Services.AddSingleton<ISqlConnectionFactory>(_ => new SqlConnectionFactory(
        builder.Configuration.GetConnectionString("Worker") ?? throw new InvalidOperationException("ConnectionStrings:OutboxDb is not configured")));

        builder.Services.AddSingleton<ISqlOutboxRepository, SqlOutboxRepository>();

        builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("Worker")));

        builder.Services.AddScoped<IOutboxRepository, OutboxRepository>();

        builder.Services.AddControllers();
        // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddOpenApi();

        builder.Services.AddHostedService<OutboxMessageWorker>();


        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();


        app.MapControllers();

        app.Run();
    }
}
