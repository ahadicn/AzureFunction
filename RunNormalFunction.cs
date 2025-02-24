using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OfficeOpenXml;
using SmarTrak.Models;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace SmarTrak
{
    public static class RunNormalFunction
    {
        public class ColumnMapping
        {
            public string ExcelColumn { get; set; }
            public string SqlColumn { get; set; }
        }

        public class ColumnMappings
        {
            public List<ColumnMapping> Mappings { get; set; }
        }

        [Function("BlobTriggerNormalFunction")]
        public static async Task RunUpload(
            [BlobTrigger("zip-container/{name}")] byte[] blobContent,
            string name,
            FunctionContext context)
        {
            var logger = context.GetLogger("BlobTriggerNormalFunction");
            logger.LogInformation($"Blob trigger normal function executed for blob: {name}");

            try
            {
                // Extract the ZIP contents and process .xlsx files
                await ExtractAndProcessXlsxFromZip(blobContent, logger);
            }
            catch (Exception ex)
            {
                logger.LogError($"Error processing blob: {name}. Exception: {ex.Message}");
            }
        }

        private static async Task ExtractAndProcessXlsxFromZip(byte[] blobContent, ILogger logger)
        {
            logger.LogInformation("Extracting and processing ZIP file.");

            using (var zipStream = new MemoryStream(blobContent))
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read))
            {
                foreach (var entry in archive.Entries)
                {
                    // Only process .xlsx files
                    if (entry.FullName.EndsWith(".xlsx"))
                    {
                        logger.LogInformation($"Processing file: {entry.FullName}");

                        using (var entryStream = entry.Open())
                        {
                            await ProcessExcel(entryStream, logger);
                        }
                    }
                }
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
