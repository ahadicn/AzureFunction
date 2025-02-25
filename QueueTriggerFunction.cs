using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

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
            logger.LogInformation($"Queue trigger function processed message: {queueMessage}");

            try
            {
                // Deserialize the message
                var messageData = JsonSerializer.Deserialize<QueueMessage>(queueMessage);

                if (messageData == null || string.IsNullOrEmpty(messageData.FileName))
                {
                    logger.LogError("Invalid message format: Missing FileName.");
                    return;
                }

                logger.LogInformation($"Processing file: {messageData.FileName}");
                // TODO: Add your processing logic here
            }
            catch (JsonException ex)
            {
                logger.LogError($"JSON Deserialization Error: {ex.Message}");
            }
            catch (Exception ex)
            {
                logger.LogError($"Unexpected Error: {ex.Message}");
            }
        }
    }

    // Define a strongly-typed model for the queue message
    public class QueueMessage
    {
        public string FileName { get; set; }
    }
}
