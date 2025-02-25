using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public class BlobTriggerDurableFunction
    {
        private readonly ILogger<BlobTriggerDurableFunction> _logger;

        public BlobTriggerDurableFunction(ILogger<BlobTriggerDurableFunction> logger)
        {
            _logger = logger;
        }

        [Function("BlobTriggerDurableFunction")]
        public async Task Run(
            [BlobTrigger("zip-container/{blobName}")] string blobName,
            [DurableClient] DurableTaskClient starter)
        {
            _logger.LogInformation($"Blob trigger activated: {blobName}");

            try
            {
                // 🚀 Pass only blob name to the orchestrator
                await starter.ScheduleNewOrchestrationInstanceAsync("OrchestratorFunction_HelloSequence", blobName);
                _logger.LogInformation($"Started Orchestrator for blob: {blobName}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error triggering orchestrator: {ex.Message}");
                throw;
            }
        }
    }
}
