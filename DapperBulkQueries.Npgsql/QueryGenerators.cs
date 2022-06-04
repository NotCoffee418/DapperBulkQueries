namespace DapperBulkQueries.Npgsql;

public static class QueryGenerators
{
    public static List<(string Query, DynamicParameters Parameters)> GenerateBulkInsert<T>(
    string tableName,
    List<T> rowObjects,
    List<string> columnNames,
    Dictionary<string, Func<T, object>> calculatedProperties = null,
    uint batchSize = 100)
    where T : class
    {
        // Preparation
        List<(string Query, DynamicParameters Parameters)> result = new();
        
        var sqlBase = $"INSERT INTO {tableName} ({string.Join(",", columnNames)}) VALUES ";
        var sqlBuilder = new StringBuilder(sqlBase);
        var parameters = new DynamicParameters();
        int batchCount = 0;

        // Iterate through the items
        for (int i = 0; i < rowObjects.Count; i++)
        {
            batchCount++;
            sqlBuilder.Append("(");

            // Handle plain properties
            foreach (var propertyName in columnNames)
            {
                object value = GetPropertyValue(propertyName, rowObjects[i], calculatedProperties);
                parameters.Add($"@{propertyName}_{i}", value);
                sqlBuilder.Append($"@{propertyName}_{i},");
            }
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append("),");

            // Execute batch if batch count is exceeded and batching is enabled
            // Or if it's the final item
            if (i == rowObjects.Count - 1 || (batchCount != 0 && batchCount >= batchSize))
            {
                // Remove trailing comma, add ; & execute
                sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
                sqlBuilder.Append(';');
                result.Add((sqlBuilder.ToString(), parameters));

                // Reset
                batchCount = 0;
                sqlBuilder.Clear();
                sqlBuilder.Append(sqlBase);
                parameters = new DynamicParameters();
            }
        }
        return result;
    }


    public static (string Query, DynamicParameters Parameters) GenerateBulkDelete<T>(
        string tableName, string selectorColumnName, List<T> selectorValues)
    {
        // Generate parameters
        var parameters = new DynamicParameters();
        for (int i = 0; i < selectorValues.Count; i++)
            parameters.Add($"@{selectorColumnName}_{i}", selectorValues[i]);

        // Generate query string
        var sqlBuilder = new StringBuilder($"DELETE FROM {tableName} WHERE {selectorColumnName} IN (");
        sqlBuilder.Append(string.Join(',',
            Enumerable.Range(0, selectorValues.Count).Select(x => $"@{selectorColumnName}_{x}")));
        sqlBuilder.Append(");");

        // Return
        return (sqlBuilder.ToString(), parameters);        
    }

    internal static (string query, DynamicParameters parameters) GenerateBulkUpdate<T>(
        string tableName,
        List<T> rowObjects, 
        List<string> selectorColumnNames, 
        List<string> columnNamesToUpdate, 
        Dictionary<string, Func<T, object>> calculatedProperties = null)
    {
        // Validate
        if (selectorColumnNames.Count < 1)
            throw new ArgumentException("GenerateBulkUpdate received no selector properties");
        if (selectorColumnNames.Count < 1)
            throw new ArgumentException("GenerateBulkUpdate received no properties to update");
        if (string.IsNullOrEmpty(tableName))
            throw new ArgumentException("GenerateBulkUpdate received no table name");
        if (rowObjects.Count == 0)
            return ("", new DynamicParameters()); // Nothing to do

        // Generate query
        List<(string query, DynamicParameters parameters)> result = new();
        var sqlBuilder = new StringBuilder("BEGIN;" + Environment.NewLine);
        var parameters = new DynamicParameters();
        for (int i = 0; i < rowObjects.Count; i++)
        {
            sqlBuilder.Append($"UPDATE {tableName} SET ");
            
            // Handle plain properties
            foreach (var columnName in columnNamesToUpdate)
            {
                object value = GetPropertyValue(columnName, rowObjects[i], calculatedProperties);
                parameters.Add($"@{columnName}_{i}", value);
                sqlBuilder.Append($"{columnName} = @{columnName}_{i},");
            }

            // Remove trailing comma on SETs & add WHERE
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append($" WHERE {string.Join(" AND ", selectorColumnNames.Select(p => $"{p} = @{p}_{i}"))};");
            foreach (var propertyName in selectorColumnNames)
            {
                var value = rowObjects[i].GetType().GetProperty(propertyName).GetValue(rowObjects[i]);
                parameters.Add($"@{propertyName}_{i}", value);
            }

            // Complete the row
            sqlBuilder.Append(";");
        }

        // Complete the query and return
        sqlBuilder.Append("COMMIT;");
        return (sqlBuilder.ToString(), parameters);
    }

    
    /* --- HELPER FUNCTIONS --- */
    /// <summary>
    /// Gets the property value of an instance of T. 
    /// Will use calculated property if any are defined.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="propertyName"></param>
    /// <param name="item"></param>
    /// <param name="calculatedProperties"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    private static object GetPropertyValue<T>(
        string propertyName,
        T item,
        Dictionary<string, Func<T, object>> calculatedProperties)
    {
        // Extract value from calculated property
        if (calculatedProperties is not null && calculatedProperties.ContainsKey(propertyName))
            return calculatedProperties[propertyName](item);

        // Extract value from class property
        else
        {
            var property = item.GetType().GetProperty(propertyName);
            if (property is null)
                throw new ArgumentException(
                    $"Failed to find property or calculated property for '{propertyName}");
            return property.GetValue(item);
        }
    }
}
