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
}
