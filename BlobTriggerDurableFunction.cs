using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.DurableTask.ContextImplementations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public class BlobTriggerFunction
    {
        private readonly IDurableClientFactory _durableClientFactory;
        private readonly ILogger<BlobTriggerFunction> _logger;

        public BlobTriggerFunction(IDurableClientFactory durableClientFactory, ILogger<BlobTriggerFunction> logger)
        {
            _durableClientFactory = durableClientFactory;
            _logger = logger;
        }

        [Function("BlobTriggerFunction")]
        public async Task Run(
            [BlobTrigger("zip-container/{name}", Connection = "AzureWebJobsStorage")] byte[] zipData,
            string name,
            FunctionContext context)
        {
            if (zipData == null || zipData.Length == 0)
            {
                _logger.LogError("ZIP file data is null or empty.");
                return;
            }

            try
            {
                var durableClient = _durableClientFactory.CreateClient();
                if (durableClient == null)
                {
                    _logger.LogError("Durable Orchestration Client is null.");
                    return;
                }

                _logger.LogInformation($"ZIP file {name} uploaded, starting processing...");
                using var zipStream = new MemoryStream(zipData);

                string instanceId = await durableClient.StartNewAsync("Orchestrator", zipStream);
                _logger.LogInformation($"Started Durable Orchestrator with ID = '{instanceId}'");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error starting Durable Orchestrator: {ex.Message}");
            }
        }
    }
}
