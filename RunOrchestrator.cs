using System;
using System.Collections.Generic;
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
            var blobName = context.GetInput<string>();

            if (string.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException(nameof(blobName), "Blob name cannot be null or empty.");
            }

            context.SetCustomStatus($"Processing blob: {blobName}");

            try
            {
                // ✅ Step 1: Download Blob
                byte[] blobContent = await context.CallActivityAsync<byte[]>("DownloadBlobActivity", blobName);

                // ✅ Step 2: Split ZIP into Excel file batches
                var batches = await context.CallActivityAsync<List<byte[]>>("SplitBlobIntoBatches", blobContent);

                // ✅ Step 3: Process Each Batch
                foreach (var batch in batches)
                {
                    await context.CallActivityAsync("ProcessBatch", batch);
                    await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(1), CancellationToken.None);
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
