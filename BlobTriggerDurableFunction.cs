using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using DurableClientAttribute = Microsoft.Azure.Functions.Worker.DurableClientAttribute;

namespace SmarTrak
{
    public static class BlobTriggerFunction
    {
        [Function("BlobTriggerFunction")]
        public static async Task Run(
            [BlobTrigger("zip-container/{name}", Connection = "AzureWebJobsStorage")] Stream zipStream,
            string name,
            FunctionContext context,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            log.LogInformation($"ZIP file {name} uploaded, starting processing...");
            string instanceId = await starter.StartNewAsync("OrchestratorFunction", zipStream);
            log.LogInformation($"Started Durable Orchestrator with ID = '{instanceId}'");
        }
    }
}
