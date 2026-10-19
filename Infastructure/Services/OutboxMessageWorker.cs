using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Infastructure.Services;

public class OutboxMessageWorker:  BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        //logger.LogInformation("MetricsWorker started");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
               // await UpdateMetricsAsync(stoppingToken);
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
            catch (Exception ex)
            {
               // logger.LogError(ex, "Error in MetricsWorker");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

       // logger.LogInformation("MetricsWorker stopped");
    }
}
