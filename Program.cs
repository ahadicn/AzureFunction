using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

// Application Insights isn't enabled by default. See https://aka.ms/AAt8mw4.
// builder.Services
//     .AddApplicationInsightsTelemetryWorkerService()
//     .ConfigureFunctionsApplicationInsights();



var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .Build();
host.Run();



builder.Build().Run();
