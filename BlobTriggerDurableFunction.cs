using System;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public class BlobTriggerDurableFunction
    {
        private readonly ILogger<BlobTriggerDurableFunction> _logger;
        public BlobTriggerDurableFunction(ILogger<BlobTriggerDurableFunction> logger) => _logger = logger;

        [Function("BlobTriggerDurableFunction")]
        public async Task Run(
            [BlobTrigger("zip-container/{name}")] byte[] blobContent,
            string name,
            [DurableClient] DurableTaskClient starter)
        {
            _logger.LogInformation($"Blob trigger activated: {name}");
            string blobBase64;
            try
            {
                blobBase64 = Convert.ToBase64String(blobContent);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error converting blob to Base64: {ex.Message}");
                throw;
            }

            var input = new BlobProcessingInputModel { BlobBase64 = blobBase64, Name = name };
            await starter.ScheduleNewOrchestrationInstanceAsync("OrchestratorFunction_HelloSequence", input);
            _logger.LogInformation($"Started Orchestrator for blob: {name}");
        }
    }

}
