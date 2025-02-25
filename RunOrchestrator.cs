using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using OrchestrationTriggerAttribute = Microsoft.Azure.Functions.Worker.OrchestrationTriggerAttribute;

namespace SmarTrak
{
    public static class OrchestratorFunction
    {
        [Function("OrchestratorFunction")]
        public static async Task OrchestratorFunc(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var zipStream = context.GetInput<Stream>();
            var extractedFiles = await context.CallActivityAsync<List<byte[]>>("ExtractXLSX", zipStream);
            foreach (var fileData in extractedFiles)
            {
                await context.CallActivityAsync("InsertIntoSQL", fileData);
            }
        }
    }
}
