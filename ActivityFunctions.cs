using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OfficeOpenXml;
using static SmarTrak.RunNormalFunction;

namespace SmarTrak
{
    public static class ActivityFunctions
    {
        [Function("SplitBlobIntoBatches")]
        public static List<byte[]> SplitBlobIntoBatches(
            [ActivityTrigger] byte[] blobContent,
            FunctionContext context)
        {
            var logger = context.GetLogger("SplitBlobIntoBatches");
            logger.LogInformation("Splitting blob into batches.");

            var batches = new List<byte[]>();
            using var zipStream = new MemoryStream(blobContent);
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);

            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.EndsWith(".xlsx"))
                {
                    using var entryStream = entry.Open();
                    using var memoryStream = new MemoryStream();
                    entryStream.CopyTo(memoryStream);
                    batches.Add(memoryStream.ToArray());
                }
            }
            return batches;
        }

        [Function("ProcessBatch")]
        public static async Task ProcessBatch(
            [ActivityTrigger] byte[] batchContent,
            FunctionContext context)
        {
            var logger = context.GetLogger("ProcessBatch");
            using var batchStream = new MemoryStream(batchContent);
            await ProcessExcel(batchStream, logger);
        }

        private static async Task ProcessExcel(Stream excelStream, ILogger logger)
        {
            logger.LogInformation("Processing Excel file.");
            try
            {
                var columnMappings = await LoadColumnMappingsAsync("columnMappings.json");
                if (columnMappings == null)
                {
                    logger.LogError("Failed to load column mappings.");
                    return;
                }

                ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                using var package = new ExcelPackage(excelStream);
                var worksheet = package.Workbook.Worksheets[0];

                int rowCount = worksheet.Dimension.Rows;
                var columnIndices = new Dictionary<string, int>();
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var columnName = worksheet.Cells[1, col].Text.Trim();
                    foreach (var mapping in columnMappings.Mappings)
                    {
                        if (columnName.Equals(mapping.ExcelColumn, StringComparison.OrdinalIgnoreCase))
                        {
                            columnIndices[mapping.SqlColumn] = col;
                        }
                    }
                }

                var records = new List<Dictionary<string, object>>();
                for (int row = 2; row <= rowCount; row++)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var mapping in columnMappings.Mappings)
                    {
                        if (columnIndices.TryGetValue(mapping.SqlColumn, out int columnIndex))
                        {
                            record[mapping.SqlColumn] = worksheet.Cells[row, columnIndex].GetValue<object>();
                        }
                    }
                    records.Add(record);
                }

                await InsertRecordsIntoDatabase(records, logger);
            }
            catch (Exception ex)
            {
                logger.LogError($"Error processing Excel file: {ex.Message}");
            }
        }

        private static async Task InsertRecordsIntoDatabase(List<Dictionary<string, object>> records, ILogger logger)
        {
            var connectionString = Environment.GetEnvironmentVariable("SQLConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                logger.LogError("Missing SQL connection string.");
                return;
            }

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
                logger.LogInformation("Inserted record successfully.");
            }
        }

        private static async Task<ColumnMappings> LoadColumnMappingsAsync(string filePath)
        {
            // Load and deserialize the JSON configuration file
            var FilePath = Path.Combine(Directory.GetCurrentDirectory(), filePath);
            using (var reader = new StreamReader(FilePath))
            {
                var json = await reader.ReadToEndAsync();
                return JsonConvert.DeserializeObject<ColumnMappings>(json);
            }
        }

    }
}
