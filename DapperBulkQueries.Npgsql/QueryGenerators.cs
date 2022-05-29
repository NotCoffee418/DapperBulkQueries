namespace DapperBulkQueries.Npgsql;

public static class QueryGenerators
{
    public static List<(string Query, DynamicParameters Parameters)> GenerateBulkInsert<T>(
    string tableName,
    List<T> rowObjects,
    List<string> propertyNames,
    Dictionary<string, Func<T, object>> calculatedProperties = null,
    uint batchSize = 100)
    where T : class
    {
        // Preparation
        List<(string Query, DynamicParameters Parameters)> result = new();
        List<string> allColumnNames = calculatedProperties is null ? propertyNames :
            propertyNames.Concat(calculatedProperties.Keys).ToList();
        
        var sqlBase = $"INSERT INTO {tableName} ({string.Join(",", allColumnNames)}) VALUES ";
        var sqlBuilder = new StringBuilder(sqlBase);
        var parameters = new DynamicParameters();
        int batchCount = 0;

        // Iterate through the items
        for (int i = 0; i < rowObjects.Count; i++)
        {
            batchCount++;
            sqlBuilder.Append("(");

            // Handle plain properties
            foreach (var propertyName in propertyNames)
            {
                var value = rowObjects[i].GetType().GetProperty(propertyName).GetValue(rowObjects[i]);
                parameters.Add($"@{propertyName}_{i}", value);
                sqlBuilder.Append($"@{propertyName}_{i},");
            }
            // Handle calculated properties
            if (calculatedProperties is not null)
                foreach ((string propertyName, Func<T, object> getValue) in calculatedProperties)
                {
                    object value = getValue(rowObjects[i]);
                    parameters.Add($"@{propertyName}_{i}", value);
                    sqlBuilder.Append($"@{propertyName}_{i},");
                }
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append("),");

            // Execute batch if batch count is exceeded and batching is enabled
            // Or if it's the final item
            if (i == rowObjects.Count - 1 || (batchCount != 0 && batchCount >= batchSize))
            {
                // Remove trailing comma & execute
                sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
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
        sqlBuilder.Append(")");

        // Return
        return (sqlBuilder.ToString(), parameters);        
    }

    internal static (string query, DynamicParameters parameters) GenerateBulkUpdate<T>(
        string tableName,
        List<T> rowObjects, 
        List<string> selectorProperties, 
        List<string> propertyNamesToUpdate, 
        Dictionary<string, Func<T, object>> calculatedProperties)
    {
        List<(string query, DynamicParameters parameters)> result = new();
        var sqlBuilder = new StringBuilder("BEGIN;" + Environment.NewLine);
        var parameters = new DynamicParameters();
        for (int i = 0; i < rowObjects.Count; i++)
        {
            sqlBuilder.Append($"UPDATE {tableName} SET ");
            // Handle plain properties
            foreach (var propertyName in propertyNamesToUpdate)
            {
                var value = rowObjects[i].GetType().GetProperty(propertyName).GetValue(rowObjects[i]);
                parameters.Add($"@{propertyName}_{i}", value);
                sqlBuilder.Append($"{propertyName} = @{propertyName}_{i},");
            }
            // Handle calculated properties
            if (calculatedProperties is not null)
                foreach ((string propertyName, Func<T, object> getValue) in calculatedProperties)
                {
                    object value = getValue(rowObjects[i]);
                    parameters.Add($"@{propertyName}_{i}", value);
                    sqlBuilder.Append($"{propertyName} = @{propertyName}_{i},");
                }

            // Remove trailing comma on SETs & add WHERE
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append($" WHERE {string.Join(" AND ", selectorProperties.Select(p => $"{p} = @{p}_{i}"))};");
            foreach (var propertyName in selectorProperties)
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
}
