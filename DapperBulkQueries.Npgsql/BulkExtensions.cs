namespace DapperBulkQueries.Npgsql;

public static class BulkExtensions
{
    /// <summary>
    /// Inserts multiple rows into a table
    /// </summary>
    /// <typeparam name="T">Corresponding class for the table</typeparam>
    /// <param name="tableName"></param>
    /// <param name="rowObjects">Class to extract property names from.</param>
    /// <param name="columnNames">Property names which have corresponding columns that should be included in the insert</param>
    /// <param name="calculatedProperties">
    /// Properties that need to be calculated with a function.
    /// Dict of column name and Function of input type outputting the value to be inserted.
    /// Properties should still be defined in propertyNames. calculatedProperties acts as an override.
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
        List<T> rowObjects,
        List<string> columnNames,
        Dictionary<string, Func<T, object>> calculatedProperties = null,
        uint batchSize = 100)
        where T : class
    {
        // Nothing to do
        if (rowObjects.Count == 0)
            return;        
        // No columns specified
        if (columnNames.Count == 0)
            throw new ArgumentException("Can't insert with no columns specified");

        // Generate queries
        var batches = QueryGenerators.GenerateBulkInsert(
            tableName, rowObjects, columnNames, calculatedProperties, batchSize);

        // Execute all batches
        foreach ((string query, DynamicParameters parameters) in batches)
            await conn.ExecuteAsync(query, parameters);
    }

    /// <summary>
    /// Deletes multiple rows from a table by a list of values.
    /// DELETE FROM {tableName} WHERE {columnName} IN ({selectorValues})
    /// </summary>
    /// <typeparam name="T">Unlike Insert and Delete, T is expected to be just the value type of the column/property, rather than the entire object.</typeparam>
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
        // Do nothing if there are no selectors
        if (selectorValues.Count == 0)
            return Task.CompletedTask;

        // Generate & execute
        (string query, DynamicParameters parameters) = QueryGenerators.GenerateBulkDelete(
            tableName, selectorColumnName, selectorValues);
        return conn.ExecuteAsync(query, parameters);
    }

    /// <summary>
    /// Updates multiple rows in a table, using selectorProperties as it's identifying columns.
    /// The updates are executed as a single transaction, causing all or none to fail.
    /// </summary>
    /// <typeparam name="T">Corresponding class for the table</typeparam>
    /// <param name="conn"></param>
    /// <param name="tableName"></param>
    /// <param name="rowObjects"></param>
    /// <param name="selectorColumns">
    /// Used to identify a row to update.
    /// These are the properties for the WHERE clauses, using AND between each.
    /// </param>
    /// <param name="columnsToUpdate"></param>
    /// <param name="calculatedProperties">
    /// Properties that need to be calculated with a function.
    /// Dict of column name and Function of input type outputting the value to be inserted.
    /// Properties should still be defined in propertyNames. calculatedProperties acts as an override.
    /// </param>
    /// <param name="useTransaction">Revert all changes if something goes wrong</param>
    /// <returns></returns>
    public static Task ExecuteBulkUpdateAsync<T>(
        this NpgsqlConnection conn,
        string tableName,
        List<T> rowObjects,
        List<string> selectorColumns,
        List<string> columnsToUpdate,
        Dictionary<string, Func<T, object>> calculatedProperties = null,
        bool useTransaction = true)
        where T : class
    {
        // Do nothing if there is nothing to update
        if (rowObjects.Count == 0)
            return Task.CompletedTask;
        // No columns specified
        if (columnsToUpdate.Count == 0)
            throw new ArgumentException("Can't update with no columns specified");
        // Ensure we have a selector    
        if (selectorColumns.Count == 0)
            throw new ArgumentException(
                "At least one selector column must be specified.");

        // Generate & execute
        (string query, DynamicParameters parameters) = QueryGenerators.GenerateBulkUpdate(
            tableName, rowObjects, selectorColumns, columnsToUpdate, calculatedProperties, useTransaction);
        return conn.ExecuteAsync(query, parameters);
    }
}
