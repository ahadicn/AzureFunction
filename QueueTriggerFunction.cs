using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using SmarTrak.Services;

namespace SmarTrak
{
    public static class QueueTriggerFunction
    {
        [Function("QueueTriggerFunction")]
        public static async Task Run(
            [QueueTrigger("zip-processing-queue", Connection = "AzureWebJobsStorage")] string queueMessage,
            FunctionContext context)
        {
            var logger = context.GetLogger("QueueTriggerFunction");
            logger.LogInformation($"Queue message received: {queueMessage}");

            try
            {
                var messageData = JsonSerializer.Deserialize<QueueMessage>(queueMessage);
                if (messageData == null || string.IsNullOrEmpty(messageData.FileName))
                {
                    logger.LogError("Invalid message: Missing FileName.");
                    return;
                }

                string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                var blobClient = new BlobContainerClient(storageConnectionString, "zip-container");
                var blob = blobClient.GetBlobClient(messageData.FileName);

                using var memoryStream = new MemoryStream();
                await blob.DownloadToAsync(memoryStream);
                memoryStream.Position = 0;

                using var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read);
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                    {
                        logger.LogInformation($"Extracting file: {entry.FullName}");

                        using var xlsxStream = entry.Open();
                        var result = await ExcelProcessor.ProcessXlsx(xlsxStream);
                        logger.LogInformation($"Processed {result} records from {entry.FullName}");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Processing error: {ex.Message}");
            }
        }
    }

    public class QueueMessage
    {
        public string FileName { get; set; }
    }
}
