

namespace DapperBulkQueries.Tests;

public class QueryGeneratorTests
{
    [Fact]
    public void GenerateBulkInsert_InsertInBatches_VerifyMultipleInserts()
    {
        List<TestTable> sampleData = SampleDataHelper.GetSampleTestTablesWithoutId1();

        // Define properties to use
        List<string> properties = new() { "TextCol", "NumberCol", "BoolCol" };

        // Insert using the extension method
        QueryGeneratorBase gen = new PgQueryGenerator();
        var insertData = gen.GenerateBulkInsert("TestTable", sampleData, properties, batchSize: 2);

        // Validate that there are 2 batches for our 3 entries with batch size of 2
        Assert.Equal(2, insertData.Count());
    }

}
