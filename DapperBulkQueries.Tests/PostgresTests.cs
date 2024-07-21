using System.Text;

namespace DapperBulkQueries.Tests;

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
        List<string> properties = new() { "NumberCol", "BoolCol", "TextCol" };

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

    [Fact]
    public async Task ExecuteBulkInsertAsync_InsertDoNothingOnConflict()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithId1();

        // Define properties to use
        List<string> properties = new() { "Id", "TextCol", "NumberCol", "BoolCol" };

        // Insert using the extension method
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, properties, batchSize: 2, onConflict: OnConflict.DoNothing);

        // Insert everything again
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, properties, batchSize: 2, onConflict: OnConflict.DoNothing);

        // Retrieve the inserted data
        var insertedData = await conn.QueryAsync<TestTable>(
            "SELECT * FROM TestTable ORDER BY Id");

        // Validate that the data matches
        Assert.Equal(sampleData.Count, insertedData.Count());
        for (int i = 0; i < sampleData.Count; i++)
        {
            Assert.Equal(sampleData[i].Id, insertedData.ElementAt(i).Id);
            Assert.Equal(sampleData[i].TextCol, insertedData.ElementAt(i).TextCol);
            Assert.Equal(sampleData[i].NumberCol, insertedData.ElementAt(i).NumberCol);
            Assert.Equal(sampleData[i].BoolCol, insertedData.ElementAt(i).BoolCol);
        }
    }

    [Fact]
    public async Task ExecuteBulkInsertAsync_InserExpectErrorOnConflict()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithId1();

        // Define properties to use
        List<string> properties = new() { "Id", "TextCol", "NumberCol", "BoolCol" };

        // Insert using the extension method
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, properties, batchSize: 2, onConflict: OnConflict.DoNothing);

        // Insert again and expect error
        var exception = await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, properties, batchSize: 2, onConflict: OnConflict.Error);
        });
        Assert.Equal("23505", exception.SqlState);

        // Retrieve the inserted data
        var insertedData = await conn.QueryAsync<TestTable>(
            "SELECT * FROM TestTable ORDER BY Id");

        // Validate that the data matches
        Assert.Equal(sampleData.Count, insertedData.Count());
        for (int i = 0; i < sampleData.Count; i++)
        {
            Assert.Equal(sampleData[i].Id, insertedData.ElementAt(i).Id);
            Assert.Equal(sampleData[i].TextCol, insertedData.ElementAt(i).TextCol);
            Assert.Equal(sampleData[i].NumberCol, insertedData.ElementAt(i).NumberCol);
            Assert.Equal(sampleData[i].BoolCol, insertedData.ElementAt(i).BoolCol);
        }
    }

    [Fact]
    public async Task ExecuteBulkDeleteAsync_ExpectRowsGone()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Insert data
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, new() { "TextCol", "NumberCol", "BoolCol" });

        // Delete data
        var rowsWithTextColToDelete = new List<string>() { "aaa", "ccc" };
        await conn.ExecuteBulkDeleteAsync("TestTable", "TextCol", rowsWithTextColToDelete);

        // Retrieve the remaining inserted data
        var remainingRows = await conn.QueryAsync<TestTable>("SELECT * FROM TestTable ORDER BY Id");

        // Validate
        Assert.NotNull(remainingRows);
        Assert.Equal(1, remainingRows?.Count());
        Assert.Equal("bbb", remainingRows?.First().TextCol);
    }

    [Fact]
    public async Task ExecuteBulkUpdateAsync_ExpectRowsChanged()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Insert data
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, new() { "TextCol", "NumberCol", "BoolCol" });

        // Updated version of first and second
        var updateData = new List<TestTable>()
        {
            new TestTable() { Id = 1, TextCol = "Updated first", NumberCol = 5, BoolCol = true },
            new TestTable() { Id = 2, TextCol = "Updated second", NumberCol = 6, BoolCol = false }
        };

        // Update data
        await conn.ExecuteBulkUpdateAsync(
            "TestTable",
            updateData,
            new List<string>() { "Id", "BoolCol" }, // Update where ID AND BoolCol match
            new() { "TextCol", "NumberCol", },  // Properties to update
            useTransaction: true);

        // Retrieve the data in it's current form
        var changedRows = (await conn.QueryAsync<TestTable>("SELECT * FROM TestTable ORDER BY Id")).ToList();

        // Validate
        Assert.NotNull(changedRows);
        Assert.Equal(3, changedRows?.Count());
        Assert.Equal("Updated first", changedRows[0].TextCol);
        Assert.Equal("Updated second", changedRows?[1].TextCol);
        Assert.Equal(5, changedRows[0].NumberCol);
        Assert.Equal(6, changedRows[1].NumberCol);
        Assert.True(changedRows[2].IsPropertiesMatch(sampleData[2]), 
            "Rows were updated but another row changed too");
    }

    [Fact]
    public async Task ExecuteBulkUpdateAsync_ExpectRowsChanged_WithoutTransaction()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Insert data
        await conn.ExecuteBulkInsertAsync(
            "TestTable", sampleData, new() { "TextCol", "NumberCol", "BoolCol" });

        // Updated version of first and second
        var updateData = new List<TestTable>()
        {
            new TestTable() { Id = 1, TextCol = "Updated first", NumberCol = 5, BoolCol = true },
            new TestTable() { Id = 2, TextCol = "Updated second", NumberCol = 6, BoolCol = false }
        };

        // Update data
        await conn.ExecuteBulkUpdateAsync(
            "TestTable",
            updateData,
            new List<string>() { "Id", "BoolCol" }, // Update where ID AND BoolCol match
            new() { "TextCol", "NumberCol", },  // Properties to update
            useTransaction: false);

        // Retrieve the data in it's current form
        var changedRows = (await conn.QueryAsync<TestTable>("SELECT * FROM TestTable ORDER BY Id")).ToList();

        // Validate
        Assert.NotNull(changedRows);
        Assert.Equal(3, changedRows?.Count());
        Assert.Equal("Updated first", changedRows[0].TextCol);
        Assert.Equal("Updated second", changedRows?[1].TextCol);
        Assert.Equal(5, changedRows[0].NumberCol);
        Assert.Equal(6, changedRows[1].NumberCol);
        Assert.True(changedRows[2].IsPropertiesMatch(sampleData[2]),
            "Rows were updated but another row changed too");
    }

    [Fact]
    public async Task InsertUpdateDelete_GeneratedWithPrefix_NoErrors()
    {
        using var conn = await ConnectionHelper.GetOpenNpgsqlConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Insert data
        PgQueryGenerator gen = new PgQueryGenerator();
        var generatedInsert = gen.GenerateBulkInsert(
            DatabaseType.Npgsql,
            "TestTable",
            sampleData,
            new() { "TextCol", "NumberCol", "BoolCol" },
            paramPrefix: "i_");
        await conn.ExecuteAsync(generatedInsert[0].Query, generatedInsert[0].Parameters);

        // Update data
        var updateData = new List<TestTable>()
        {
            new TestTable() { Id = 1, TextCol = "Updated first", NumberCol = 5, BoolCol = true },
            new TestTable() { Id = 2, TextCol = "Updated second", NumberCol = 6, BoolCol = false }
        };
        var generatedUpdate = gen.GenerateBulkUpdate(
            DatabaseType.Npgsql,
            "TestTable",
            updateData,
            new List<string>() { "Id", "BoolCol" }, // Update where ID AND BoolCol match
            new() { "TextCol", "NumberCol", },
            paramPrefix: "u_"); // Properties to update
        await conn.ExecuteAsync(generatedUpdate.Query, generatedUpdate.Parameters);

        // Delete Data
        var generatedDelete = gen.GenerateBulkDelete(
            DatabaseType.Npgsql,
            "TestTable", 
            "TextCol", 
            new List<string>() { "aaa", "ccc" },
            paramPrefix: "d_");
        await conn.ExecuteAsync(generatedDelete.Query, generatedDelete.Parameters);
    }

    [Fact] // test some equivalent in mssql, then delete thiso ne
    public async Task ExecuteBulkInsertAsync_AdvancedTransaction_NoCrash()
    {
        // Queries with parameters do not work in PG
        // because the @ symbol, required to assign parameters
        // has a different meaning causing it to conflict.
    }
}
