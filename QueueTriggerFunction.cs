using System;
using System.IO;
using System.IO.Compression;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public class QueueTriggerFunction
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ILogger<QueueTriggerFunction> _logger;

        public QueueTriggerFunction(ILogger<QueueTriggerFunction> logger)
        {
            _logger = logger;
            _blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
        }

        [Function("QueueTriggerFunction")]
        public async Task Run(
            [QueueTrigger("zip-processing-queue", Connection = "AzureWebJobsStorage")] string queueMessage)
        {
            var message = JsonSerializer.Deserialize<dynamic>(queueMessage);
            string fileName = message?.FileName;

            if (string.IsNullOrEmpty(fileName))
            {
                _logger.LogError("Invalid queue message.");
                return;
            }

            try
            {
                _logger.LogInformation($"Processing ZIP file: {fileName}");

                var containerClient = _blobServiceClient.GetBlobContainerClient("zip-container");
                var blobClient = containerClient.GetBlobClient(fileName);

                using var memoryStream = new MemoryStream();
                await blobClient.DownloadToAsync(memoryStream);
                memoryStream.Position = 0;

                using var archive = new ZipArchive(memoryStream);
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                    {
                        _logger.LogInformation($"Extracting: {entry.FullName}");
                        // TODO: Process XLSX and insert into SQL Server
                    }
                }

                _logger.LogInformation($"Finished processing {fileName}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing ZIP file: {ex.Message}");
            }
        }
    }
}
