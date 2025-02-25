using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Queues;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public class BlobTriggerFunction
    {
        private readonly QueueClient _queueClient;
        private readonly ILogger<BlobTriggerFunction> _logger;

        public BlobTriggerFunction(ILogger<BlobTriggerFunction> logger)
        {
            _logger = logger;
            _queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "zip-processing-queue");
            _queueClient.CreateIfNotExists();
        }

        [Function("BlobTriggerFunction")]
        public async Task Run(
            [BlobTrigger("zip-container/{name}", Connection = "AzureWebJobsStorage")] byte[] zipData,
            string name)
        {
            if (zipData == null || zipData.Length == 0)
            {
                _logger.LogError("ZIP file data is null or empty.");
                return;
            }

            try
            {
                _logger.LogInformation($"ZIP file {name} uploaded, adding message to queue...");

                var message = JsonSerializer.Serialize(new { FileName = name });
                await _queueClient.SendMessageAsync(Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(message)));

                _logger.LogInformation($"Added message to queue for {name}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error adding message to queue: {ex.Message}");
            }
        }
    }
}
