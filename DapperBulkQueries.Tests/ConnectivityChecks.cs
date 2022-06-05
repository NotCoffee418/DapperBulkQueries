namespace DapperBulkQueries.Tests
{
    public class ConnectivityChecks
    {
        [Fact]
        public async Task TestPostgresConnection()
        {
            var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
            bool check = await conn.ExecuteScalarAsync<bool>("SELECT true");
            Assert.True(check);            
        }

        [Fact]
        public async Task TestSqlServerConnection()
        {
            var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
            bool check = await conn.ExecuteScalarAsync<bool>("SELECT 1");
            Assert.True(check);
        }
    }
}