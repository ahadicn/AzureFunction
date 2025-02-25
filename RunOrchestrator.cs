using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public static class OrchestratorFunction
    {
        [Function("OrchestratorFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [Microsoft.Azure.Functions.Worker.OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var inputJson = context.GetInput<string>();

            if (string.IsNullOrWhiteSpace(inputJson))
                throw new ArgumentException("Received empty input JSON.");

            var input = JsonSerializer.Deserialize<BlobProcessingInputModel>(inputJson);
            if (input == null)
                throw new ArgumentException("Deserialization returned null.");

            var outputs = new List<string>();

            // Step 1: Download Blob
            byte[] zipData = await context.CallActivityAsync<byte[]>("DownloadBlobActivity", input);

            // Step 2: Extract XLSX files
            List<byte[]> xlsxFiles = await context.CallActivityAsync<List<byte[]>>("ExtractXlsxFromZipActivity", zipData);

            // Step 3: Process Excel files
            foreach (var xlsx in xlsxFiles)
            {
                string result = await context.CallActivityAsync<string>("ProcessExcelActivity", xlsx);
                outputs.Add(result);
            }

            return outputs;
        }
    }
}
