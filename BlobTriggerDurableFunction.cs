using System;
using System.IO;
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
        private readonly BlobServiceClient _blobServiceClient;

        public BlobTriggerDurableFunction(ILogger<BlobTriggerDurableFunction> logger, BlobServiceClient blobServiceClient)
        {
            _logger = logger;
            _blobServiceClient = blobServiceClient;
        }

        [Function("BlobTriggerDurableFunction")]
        public async Task Run(
            [BlobTrigger("zip-container/{name}")] string blobName,
            string name,
            [DurableClient] DurableTaskClient starter)
        {
            _logger.LogInformation($"Blob trigger activated: {name}");

            try
            {
                var containerClient = _blobServiceClient.GetBlobContainerClient("zip-container");
                var blobClient = containerClient.GetBlobClient(blobName);

                byte[] blobBytes;
                using (var memoryStream = new MemoryStream())
                {
                    await blobClient.DownloadToAsync(memoryStream);
                    blobBytes = memoryStream.ToArray();
                }

                string blobBase64 = Convert.ToBase64String(blobBytes);

                var input = new BlobProcessingInputModel { BlobBase64 = blobBase64, Name = name };
                await starter.ScheduleNewOrchestrationInstanceAsync("OrchestratorFunction_HelloSequence", input);
                _logger.LogInformation($"Started Orchestrator for blob: {name}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing blob: {ex.Message}");
                throw;
            }
        }
    }
}
