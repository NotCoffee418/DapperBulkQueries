namespace DapperBulkQueries.Npgsql;

public static class BulkExtensions
{
    /// <summary>
    /// Inserts multiple rows into a table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableName"></param>
    /// <param name="items">Class to extract property names from.</param>
    /// <param name="propertyNames">Property names which have corresponding columns that should be included in the insert</param>
    /// <param name="calculatedProperties">
    /// Properties that need to be calculated with a function
    /// Dict of column name and Function of input type outputting the value to be inserted
    /// </param>
    /// <param name="batchSize">
    /// How many inserts to bulk in a single query. (default = 100)
    /// When this amount is exceeded, multiple queries will be executed.
    /// 0 means no batching.
    /// </param>
    /// <returns></returns>
    public static async Task ExecuteBulkInsertAsync<T>(
        this NpgsqlConnection conn,
        string tableName,
        List<T> items,
        List<string> propertyNames,
        Dictionary<string, Func<T, object>> calculatedProperties = null,
        uint batchSize = 100)
        where T : class
    {
        // Generate queries
        var batches = QueryGenerators.GenerateBulkInsert(
            tableName, items, propertyNames, calculatedProperties, batchSize);

        // Execute all batches
        foreach ((string query, DynamicParameters parameters) in batches)
            await conn.ExecuteAsync(query, parameters);
    }

    /// <summary>
    /// Deletes multiple rows from a table by a list of values.
    /// DELETE FROM {tableName} WHERE {columnName} IN ({selectorValues})
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableName"></param>
    /// <param name="selectorColumnName"></param>
    /// <param name="selectorValues"></param>
    /// <returns></returns>
    public static Task ExecuteBulkDeleteAsync<T>(
        this NpgsqlConnection conn,
        string tableName,
        string selectorColumnName,
        List<T> selectorValues)
    {
        (string query, DynamicParameters parameters) = QueryGenerators.GenerateBulkDelete(
            tableName, selectorColumnName, selectorValues);
        return conn.ExecuteAsync(query, parameters);
    }
}
