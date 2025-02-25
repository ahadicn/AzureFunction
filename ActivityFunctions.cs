using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;

namespace SmarTrak
{
    public class ActivityFunctions
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly ILogger<ActivityFunctions> _logger;

        public ActivityFunctions(BlobServiceClient blobServiceClient, ILogger<ActivityFunctions> logger)
        {
            _blobServiceClient = blobServiceClient;
            _logger = logger;
        }

        public static class DownloadBlobActivity
        {
            [Function("DownloadBlobActivity")]
            public static async Task<byte[]> Run(
                [ActivityTrigger] BlobProcessingInputModel input,
                FunctionContext context)
            {
                var logger = context.GetLogger("DownloadBlobActivity");
                logger.LogInformation($"Downloading blob: {input.BlobName}");

                string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                var blobClient = new BlobClient(connectionString, input.Name, input.BlobName);

                using var memoryStream = new MemoryStream();
                await blobClient.DownloadToAsync(memoryStream);
                return memoryStream.ToArray();
            }
        }

        // ✅ Step 2: Split ZIP into Excel Batches
        [Function("SplitBlobIntoBatches")]
        public static List<byte[]> SplitBlobIntoBatches(
            [ActivityTrigger] byte[] blobContent,
            FunctionContext context)
        {
            var logger = context.GetLogger("SplitBlobIntoBatches");
            var batches = new List<byte[]>();

            try
            {
                using var zipStream = new MemoryStream(blobContent);
                using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);

                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                    {
                        using var entryStream = entry.Open();
                        using var memoryStream = new MemoryStream();
                        entryStream.CopyTo(memoryStream);
                        batches.Add(memoryStream.ToArray());
                    }
                }
                logger.LogInformation($"Extracted {batches.Count} Excel files.");
            }
            catch (Exception ex)
            {
                logger.LogError($"Error splitting ZIP: {ex.Message}");
                throw;
            }

            return batches;
        }

        // ✅ Step 3: Process Each Batch
        [Function("ProcessBatch")]
        public static async Task ProcessBatch([ActivityTrigger] byte[] batchContent, FunctionContext context)
        {
            var logger = context.GetLogger("ProcessBatch");
            try
            {
                using var batchStream = new MemoryStream(batchContent);
                await ProcessExcel(batchStream, logger);
            }
            catch (Exception ex)
            {
                logger.LogError($"Error processing batch: {ex.Message}");
                throw;
            }
        }

        // ✅ Step 4: Process Excel and Insert into SQL
        private static async Task ProcessExcel(Stream excelStream, ILogger logger)
        {
            logger.LogInformation("Processing Excel file.");

            try
            {
                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                using var package = new ExcelPackage(excelStream);
                var worksheet = package.Workbook.Worksheets[0];

                int rowCount = worksheet.Dimension.Rows;
                var records = new List<Dictionary<string, object>>();

                for (int row = 2; row <= rowCount; row++)
                {
                    var record = new Dictionary<string, object>
                    {
                        ["EmployeeId"] = worksheet.Cells[row, 1].Text,
                        ["Name"] = worksheet.Cells[row, 2].Text,
                        ["Address"] = worksheet.Cells[row, 3].Text,
                        ["Gender"] = worksheet.Cells[row, 4].Text,
                        ["Department"] = worksheet.Cells[row, 5].Text
                    };

                    records.Add(record);
                }

                await InsertRecordsIntoDatabase(records, logger);
            }
            catch (Exception ex)
            {
                logger.LogError($"Error processing Excel file: {ex.Message}");
                throw;
            }
        }

        // ✅ Step 5: Insert Data into SQL
        private static async Task InsertRecordsIntoDatabase(List<Dictionary<string, object>> records, ILogger logger)
        {
            var connectionString = Environment.GetEnvironmentVariable("SQLConnectionString");

            try
            {
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();

                foreach (var record in records)
                {
                    var columns = string.Join(", ", record.Keys);
                    var parameters = string.Join(", ", record.Keys.Select(k => $"@{k}"));
                    string query = $"INSERT INTO Employee ({columns}) VALUES ({parameters})";

                    using var cmd = new SqlCommand(query, conn);
                    foreach (var kvp in record)
                    {
                        cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value ?? DBNull.Value);
                    }

                    await cmd.ExecuteNonQueryAsync();
                }
                logger.LogInformation("Inserted records successfully.");
            }
            catch (Exception ex)
            {
                logger.LogError($"Error inserting records into database: {ex.Message}");
                throw;
            }
        }
    }
}
