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
    }
}