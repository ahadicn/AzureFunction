using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;

namespace SmarTrak
{
    public static class RunOrchestrator
    {
        [Function("OrchestratorFunction_HelloSequence")]
        public static async Task OrchestratorFunction_HelloSequence(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            // Deserialize input properly
            var input = context.GetInput<BlobProcessingInputModel>();

            if (input == null)
            {
                throw new ArgumentNullException(nameof(input), "Input is null in the orchestrator function.");
            }

            byte[] blobContent = Convert.FromBase64String(input.BlobBase64);
            string name = input.Name;

            context.SetCustomStatus($"Orchestrating for blob: {name}");

            try
            {
                var batches = await context.CallActivityAsync<List<byte[]>>("SplitBlobIntoBatches", blobContent);

                foreach (var batch in batches)
                {
                    await context.CallActivityAsync("ProcessBatch", batch);

                    var nextBatchTime = context.CurrentUtcDateTime.AddMinutes(1);
                    await context.CreateTimer(nextBatchTime, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                context.SetCustomStatus($"Error in orchestration: {ex.Message}");
                throw;
            }
        }
    }
        
}
