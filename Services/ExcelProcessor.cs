using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using ExcelDataReader;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace SmarTrak.Services
{
    public static class ExcelProcessor
    {
        public static async Task<int> ProcessXlsx(Stream xlsxStream)
        {
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
            int recordCount = 0;

            using (var reader = ExcelReaderFactory.CreateReader(xlsxStream))
            {
                while (reader.Read())
                {
                    if (reader.Depth == 0) continue; // Skip header row

                    var employeeId = reader.GetValue(0)?.ToString();
                    var name = reader.GetValue(1)?.ToString();
                    var address = reader.GetValue(2)?.ToString();
                    var gender = reader.GetValue(3)?.ToString();
                    var department = reader.GetValue(4)?.ToString();

                    await InsertIntoDatabase(employeeId, name, address, gender, department);
                    recordCount++;
                }
            }

            return recordCount;
        }

        private static async Task InsertIntoDatabase(string employeeId, string name, string address, string gender, string department)
        {
            var connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand("INSERT INTO Employees (EmployeeId, Name, Address, Gender, Department) VALUES (@EmployeeId, @Name, @Address, @Gender, @Department)", connection))
                {
                    command.Parameters.AddWithValue("@EmployeeId", employeeId);
                    command.Parameters.AddWithValue("@Name", name);
                    command.Parameters.AddWithValue("@Address", address);
                    command.Parameters.AddWithValue("@Gender", gender);
                    command.Parameters.AddWithValue("@Department", department);
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }
}
