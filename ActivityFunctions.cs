using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using ExcelDataReader;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.WebJobs;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using OfficeOpenXml;

namespace SmarTrak
{
    public static class ActivityFunctions
    {
        [FunctionName("ExtractXLSX")]
        public static List<byte[]> ExtractXLSX([ActivityTrigger] Stream zipStream, ILogger log)
        {
            List<byte[]> excelFiles = new();
            using (var archive = new ZipArchive(zipStream))
            {
                foreach (var entry in archive.Entries.Where(e => e.FullName.EndsWith(".xlsx")))
                {
                    using var ms = new MemoryStream();
                    using var entryStream = entry.Open();
                    entryStream.CopyTo(ms);
                    excelFiles.Add(ms.ToArray());
                    log.LogInformation($"Extracted file: {entry.FullName}");
                }
            }
            return excelFiles;
        }

        [FunctionName("InsertIntoSQL")]
        public static async Task InsertIntoSQL([ActivityTrigger] byte[] excelData, ILogger log)
        {
            using var ms = new MemoryStream(excelData);
            using var reader = ExcelReaderFactory.CreateReader(ms);
            using var conn = new SqlConnection(Environment.GetEnvironmentVariable("SQL_CONNECTION_STRING"));
            await conn.OpenAsync();
            using var transaction = await conn.BeginTransactionAsync();
            try
            {
                while (reader.Read())
                {
                    string query = "INSERT INTO Employees (EmployeeId, Name, Address, Gender, Department) VALUES (@Id, @Name, @Address, @Gender, @Dept)";
                    using var cmd = new SqlCommand(query, conn, (SqlTransaction)transaction);
                    cmd.Parameters.AddWithValue("@Id", reader.GetValue(0));
                    cmd.Parameters.AddWithValue("@Name", reader.GetString(1));
                    cmd.Parameters.AddWithValue("@Address", reader.GetString(2));
                    cmd.Parameters.AddWithValue("@Gender", reader.GetString(3));
                    cmd.Parameters.AddWithValue("@Dept", reader.GetString(4));
                    await cmd.ExecuteNonQueryAsync();
                }
                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                log.LogError($"Error inserting into SQL: {ex.Message}");
                await transaction.RollbackAsync();
            }
        }
    }
}
