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
            [BlobTrigger("zip-container/{name}", Connection = "AzureWebJobsStorage")] ReadOnlyMemory<byte> zipData,
            string name,
            FunctionContext context,
            [DurableClient] IDurableOrchestrationClient orchestrationClient,
            ILogger log)
        {
            var logger = context.GetLogger("ExtractAndProcessZip");
            logger.LogInformation($"ZIP file {name} uploaded, starting processing...");

            using var zipStream = new MemoryStream(zipData.ToArray());

            string instanceId = await orchestrationClient.StartNewAsync("Orchestrator", zipStream);
            logger.LogInformation($"Started Durable Orchestrator with ID = '{instanceId}'");
        }
    }
}
