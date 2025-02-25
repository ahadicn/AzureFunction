using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using SmarTrak;

public static class RunOrchestrator
{
    [Function("OrchestratorFunction_HelloSequence")]
    public static async Task OrchestratorFunction_HelloSequence(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var input = context.GetInput<BlobProcessingInputModel>();
        if (input == null || string.IsNullOrEmpty(input.BlobName))
        {
            throw new ArgumentNullException(nameof(input), "Input is null or missing BlobName.");
        }

        context.SetCustomStatus($"Processing blob: {input.BlobName}");

        try
        {
            // Step 1: Fetch blob content in an activity function
            byte[] blobContent = await context.CallActivityAsync<byte[]>("DownloadBlobActivity", input.BlobName);

            // Step 2: Split blob into batches
            var batches = await context.CallActivityAsync<List<byte[]>>("SplitBlobIntoBatches", blobContent);

            // Step 3: Process each batch
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
