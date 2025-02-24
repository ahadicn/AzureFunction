using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace SmarTrak
{
    public static class GetUsers
    {
        [Function("GetUsers")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "users")] HttpRequest req,
            FunctionContext context)
        {
            var log = context.GetLogger("GetUsers");
            log.LogInformation("Fetching users...");

            string connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            var users = new List<object>();

            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    await conn.OpenAsync();
                    string query = "SELECT Id, Name, Email FROM Users";
                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                users.Add(new
                                {
                                    Id = reader["Id"],
                                    Name = reader["Name"],
                                    Email = reader["Email"]
                                });
                            }
                        }
                    }
                }
            }
            catch (SqlException ex)
            {
                log.LogError($"SQL error: {ex.Message}");
                return new StatusCodeResult(500);
            }

            return new OkObjectResult(users);
        }
    }
}
