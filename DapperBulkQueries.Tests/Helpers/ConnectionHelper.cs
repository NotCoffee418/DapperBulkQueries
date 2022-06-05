namespace DapperBulkQueries.Tests.Helpers;

public static class ConnectionHelper
{
    internal static async Task<NpgsqlConnection> GetOpenNpgsqlConnectionAsync()
    {
        var connectionString = "Host=127.0.0.1;Port=9015;Username=tester;Password=tester;Database=tester";
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    internal static async Task<SqlConnection> GetOpenSqlServerConnectionAsync()
    {
        var connectionString = "Server=127.0.0.1,9016;User Id=SA;Password=tester123954;Database=Master;TrustServerCertificate=True";
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
