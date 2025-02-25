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
            [BlobTrigger("zip-container/{name}")] byte[] blobContent,
            string name,
            FunctionContext context,
            [DurableClient] IDurableOrchestrationClient starter)
        {
            var logger = context.GetLogger("BlobTriggerFunction");
            logger.LogInformation($"BlobTriggerFunction triggered for: {name}");

            var input = new BlobProcessingInputModel
            {
                BlobName = name,
                Name = "zip-container"
            };

            string jsonInput = JsonSerializer.Serialize(input);
            await starter.StartNewAsync("OrchestratorFunction", jsonInput);
        }
    }
}
