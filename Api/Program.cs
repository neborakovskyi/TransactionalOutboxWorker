
using Domain.Interfaces;
using Infastructure.Persistense;
using Infastructure.Services;
using Microsoft.EntityFrameworkCore;
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
