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
            using (var zipStream = new MemoryStream(blobContent))
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
            {
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith(".xlsx"))
                    {
                        using (var entryStream = new MemoryStream())
                        {
                            entry.Open().CopyTo(entryStream);
                            batches.Add(entryStream.ToArray());
                        }
                    }
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
            logger.LogInformation("Processing batch.");

            using (var batchStream = new MemoryStream(batchContent))
            {
                // Your existing code to process Excel files and insert data into the database
                await ProcessExcel(batchStream, logger);
            }
        }

        private static async Task ProcessExcel(Stream excelStream, ILogger logger)
        {
            logger.LogInformation("Processing Excel file.");

            try
            {
                // Load column mappings from JSON file
                var columnMappings = await LoadColumnMappingsAsync("columnMappings.json");
                if (columnMappings == null)
                {
                    logger.LogError("Failed to load column mappings.");
                    return;
                }

                ExcelPackage.LicenseContext = LicenseContext.NonCommercial; // Required for EPPlus
                using var package = new ExcelPackage(excelStream);
                var worksheet = package.Workbook.Worksheets[0];

                int rowCount = worksheet.Dimension.Rows;
                var headerRow = worksheet.Cells[1, 1, 1, worksheet.Dimension.Columns];

                // Create a dictionary to map column names to their indices
                var columnIndices = new Dictionary<string, int>();
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var columnName = worksheet.Cells[1, col].Text.Trim();
                    if (string.IsNullOrEmpty(columnName)) continue; // Skip empty headers

                    // Normalize the column name for better matching
                    var normalizedExcelColumn = columnName.ToLower().Trim();

                    foreach (var mapping in columnMappings.Mappings)
                    {
                        var normalizedMapping = mapping.ExcelColumn.ToLower().Trim();
                        if (normalizedExcelColumn == normalizedMapping)
                        {
                            columnIndices[mapping.SqlColumn] = col;
                            logger.LogInformation($"Mapped Excel Column '{columnName}' to SQL Column '{mapping.SqlColumn}' at index {col}");
                            break; // Stop checking once matched
                        }
                    }
                }

                var records = new List<Dictionary<string, object>>();
                for (int row = 2; row <= rowCount; row++)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var mapping in columnMappings.Mappings)
                    {
                        if (columnIndices.TryGetValue(mapping.ExcelColumn, out int columnIndex))
                        {
                            var cellValue = worksheet.Cells[row, columnIndex].GetValue<object>();
                            record[mapping.SqlColumn] = cellValue;
                        }
                        else
                        {
                            logger.LogWarning($"Column '{mapping.ExcelColumn}' not found in Excel file.");
                        }
                    }
                    records.Add(record);
                }

                // Retrieve SQL connection string
                var sqlConnectionString = Environment.GetEnvironmentVariable("SQLConnectionString");
                if (string.IsNullOrEmpty(sqlConnectionString))
                {
                    logger.LogError("SQLConnectionString environment variable is not set.");
                    return;
                }

                logger.LogInformation($"Retrieved SQL Connection String: {sqlConnectionString}");

                using var conn = new SqlConnection(sqlConnectionString);
                await conn.OpenAsync();

                foreach (var record in records)
                {
                    var columns = string.Join(", ", record.Keys);
                    var parameters = string.Join(", ", record.Keys.Select(k => $"@{k}"));
                    string query = $"INSERT INTO Employee ({columns}) VALUES ({parameters})";

                    using var cmd = new SqlCommand(query, conn);
                    foreach (var kvp in record)
                    {
                        cmd.Parameters.AddWithValue($"@{kvp.Key}", kvp.Value);
                    }
                    await cmd.ExecuteNonQueryAsync();
                    logger.LogInformation($"Inserted record.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError($"Error processing Excel file. Exception: {ex.Message}");
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
