namespace DapperBulkQueries.Tests
{
    public class PostgresTests : IDisposable
    {
        // Setup
        public PostgresTests()
        {
            Task.Run(async () =>
            {
                var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
                await conn.ExecuteAsync(@"
                DROP TABLE IF EXISTS TestTable;
                CREATE TABLE TestTable (
                    Id serial PRIMARY KEY,
                    TextCol character varying,
                    NumberCol numeric,
                    BoolCol boolean);");
            }).Wait();            
        }
        
        // Cleanup
        public void Dispose()
        {
            Task.Run(async () =>
            {
                var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
                await conn.ExecuteAsync(@"DROP TABLE TestTable");
            }).Wait();
        }


        [Fact]
        public async Task ExecuteBulkInsertAsync_CanReturnMatchInSameSequence()
        {
            using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
            List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

            // Define properties to use
            List<string> properties = new() { "TextCol", "NumberCol", "BoolCol" };

            // Insert using the extension method
            await conn.ExecuteBulkInsertAsync(
                "TestTable", sampleData, properties);

            // Retrieve the inserted data
            var insertedData = await conn.QueryAsync<TestTable>(
                "SELECT * FROM TestTable ORDER BY Id");

            // Validate that the data matches
            Assert.Equal(sampleData.Count, insertedData.Count());
            for (int i = 0; i < sampleData.Count; i++)
            {
                Assert.Equal(sampleData[i].TextCol, insertedData.ElementAt(i).TextCol);
                Assert.Equal(sampleData[i].NumberCol, insertedData.ElementAt(i).NumberCol);
                Assert.Equal(sampleData[i].BoolCol, insertedData.ElementAt(i).BoolCol);
            }
        }

        [Fact]
        public async Task ExecuteBulkInsertAsync_WithCalculatedProperties_ExpectsMatchingOutput()
        {
            using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
            List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

            // Define properties to use
            List<string> properties = new() { "NumberCol", "BoolCol" };

            // Calculated properties, make text depend on bool
            Dictionary<string, Func<TestTable, object>> calculatedProperties = new()
            {
                { "TextCol", t => t.BoolCol ? "Bool is True" : "Bool is False" }
            };

            // Insert using the extension method
            await conn.ExecuteBulkInsertAsync(
                "TestTable", sampleData, properties, calculatedProperties);            

            // Retrieve the inserted data
            var insertedData = await conn.QueryAsync<TestTable>(
                "SELECT * FROM TestTable ORDER BY Id");

            // Validate that the data matches
            Assert.Equal(sampleData.Count, insertedData.Count());
            foreach (var item in insertedData)
                Assert.Equal(item.TextCol, item.BoolCol ? "Bool is True" : "Bool is False");
        }

        [Fact]
        public async Task ExecuteBulkInsertAsync_InsertInBatches()
        {
            using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
            List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

            // Define properties to use
            List<string> properties = new() { "TextCol", "NumberCol", "BoolCol" };

            // Insert using the extension method
            await conn.ExecuteBulkInsertAsync(
                "TestTable", sampleData, properties, batchSize: 2);

            // Retrieve the inserted data
            var insertedData = await conn.QueryAsync<TestTable>(
                "SELECT * FROM TestTable ORDER BY Id");

            // Validate that the data matches
            Assert.Equal(sampleData.Count, insertedData.Count());
            for (int i = 0; i < sampleData.Count; i++)
            {
                Assert.Equal(sampleData[i].TextCol, insertedData.ElementAt(i).TextCol);
                Assert.Equal(sampleData[i].NumberCol, insertedData.ElementAt(i).NumberCol);
                Assert.Equal(sampleData[i].BoolCol, insertedData.ElementAt(i).BoolCol);
            }
        }
    }
}
