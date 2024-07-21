using System.Text;

namespace DapperBulkQueries.Tests;

public class SqlServerTests : IDisposable
{
    // Setup
    public SqlServerTests()
    {
        Task.Run(async () =>
        {
            var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
            await conn.ExecuteAsync(@"
                IF OBJECT_ID('dbo.TestTable', 'U') IS NOT NULL DROP TABLE dbo.TestTable; 
                CREATE TABLE dbo.TestTable (
                    Id int IDENTITY(1,1) PRIMARY KEY,
                    TextCol nvarchar(128),
                    NumberCol numeric(32,16),
                    BoolCol bit
                );");
        }).Wait();            
    }
    
    // Cleanup
    public void Dispose()
    {
        Task.Run(async () =>
        {
            var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
            await conn.ExecuteAsync(@"DROP TABLE TestTable");
        }).Wait();
    }


    [Fact]
    public async Task ExecuteBulkInsertAsync_CanReturnMatchInSameSequence()
    {
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
    public async Task ExecuteBulkDeleteAsync_ExpectRowsGone()
    {
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
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
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Insert data
        MsQueryGenerator gen = new();
        var generatedInsert = gen.GenerateBulkInsert(
            DatabaseType.SqlServer,
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
            DatabaseType.SqlServer,
            "TestTable",
            updateData,
            new List<string>() { "Id", "BoolCol" }, // Update where ID AND BoolCol match
            new() { "TextCol", "NumberCol", },
            paramPrefix: "u_"); // Properties to update
        await conn.ExecuteAsync(generatedUpdate.Query, generatedUpdate.Parameters);

        // Delete Data
        var generatedDelete = gen.GenerateBulkDelete(
            DatabaseType.SqlServer,
            "TestTable", 
            "TextCol", 
            new List<string>() { "aaa", "ccc" },
            paramPrefix: "d_");
        await conn.ExecuteAsync(generatedDelete.Query, generatedDelete.Parameters);
    }

    [Fact] // test some equivalent in mssql, then delete thiso ne
    public async Task ExecuteBulkInsertAsync_AdvancedTransaction_NoCrash()
    {
        using var conn = await ConnectionHelper.GetOpenSqlServerConnectionAsync();
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        PgQueryGenerator gen = new PgQueryGenerator();
        var generatedInsert = gen.GenerateBulkInsert(
            DatabaseType.SqlServer,
            "TestTable",
            sampleData,
            new() { "TextCol", "NumberCol", "BoolCol" },
            paramPrefix: "i_");

        // Insert with conditional update and parameters
        StringBuilder sql = new();
        DynamicParameters parameters = generatedInsert[0].Parameters;
        parameters.Add("NewValue", "ddd");
        sql.AppendLine(@"
            DECLARE @TestVariable nvarchar(3);
            DECLARE @TestCondition bit;");
        sql.AppendLine(generatedInsert[0].Query);
        sql.AppendLine(@"
            SELECT @TestCondition = 1;
            SELECT @TestVariable = @NewValue;
            IF @TestCondition = 1
            BEGIN
                UPDATE TestTable SET TextCol = @TestVariable WHERE Id = 1;
            END;");
        await conn.ExecuteAsync(sql.ToString(), parameters);

        // Validate
        TestTable changedRow = await conn.QueryFirstAsync<TestTable>("SELECT * FROM TestTable WHERE Id = 1");
        Assert.NotNull(changedRow);
        Assert.Equal("ddd", changedRow.TextCol);
        
    }
}
