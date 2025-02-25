using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public static class BlobTriggerFunction
    {
        [Function("BlobTriggerFunction")]
        public static async Task Run(
            [BlobTrigger("zip-container/{name}", Connection = "AzureWebJobsBlobStorage")] byte[] zipData,
            string name,
            FunctionContext context,
            [Microsoft.Azure.Functions.Worker.DurableClient] IDurableOrchestrationClient orchestrationClient)
        {
            var logger = context.GetLogger("ExtractAndProcessZip");

            if (zipData == null || zipData.Length == 0)
            {
                logger.LogError("ZIP file data is null or empty.");
                return;
            }

            if (orchestrationClient == null)
            {
                logger.LogError("Durable Orchestration Client is null.");
                return;
            }

            try
            {
                logger.LogInformation($"ZIP file {name} uploaded, starting processing...");
                using var zipStream = new MemoryStream(zipData);

                string instanceId = await orchestrationClient.StartNewAsync("Orchestrator", zipStream);
                logger.LogInformation($"Started Durable Orchestrator with ID = '{instanceId}'");
            }
            catch (Exception ex)
            {
                logger.LogError($"Error starting Durable Orchestrator: {ex.Message}");
            }
        }
    }
}
