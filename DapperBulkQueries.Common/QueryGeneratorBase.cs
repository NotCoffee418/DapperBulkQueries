using DapperBulkQueries.Common.Internal;

namespace DapperBulkQueries.Common;

public abstract class QueryGeneratorBase
{
    public QueryGeneratorBase(
        string transactionOpen,
        string transactionClose)
    {
        TransactionOpen = transactionOpen;
        TransactionClose = transactionClose;
    }

    /// <summary>
    /// Defines syntax for opening a transaction.
    /// Should include ; at the end if any.
    /// </summary>
    protected string TransactionOpen { get; set; }

    /// <summary>
    /// Defines syntax for closing a transaction.
    /// Should include ; at the end if any.
    /// </summary>
    protected string TransactionClose { get; set; }
    

    public virtual List<(string Query, DynamicParameters Parameters)> GenerateBulkInsert<T>(
        DatabaseType dbType,
        string tableName,
        List<T> rowObjects,
        List<string> columnNames,
        Dictionary<string, Func<T, object>>? calculatedProperties = null,
        uint batchSize = 100, 
        string paramPrefix = "",
        OnConflict onConflict = OnConflict.Error)
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
                parameters.Add($"@{paramPrefix}{propertyName}_{i}", value);
                sqlBuilder.Append($"@{paramPrefix}{propertyName}_{i},");
            }
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append("),");

            // Execute batch if batch count is exceeded and batching is enabled
            // Or if it's the final item
            if (i == rowObjects.Count - 1 || (batchCount != 0 && batchCount >= batchSize))
            {
                // Remove trailing comma, add ; & execute
                sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
                if (dbType == DatabaseType.Npgsql && onConflict == OnConflict.DoNothing)
                    sqlBuilder.Append(" ON CONFLICT DO NOTHING");
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


    public virtual (string Query, DynamicParameters Parameters) GenerateBulkDelete<T>(
        DatabaseType dbType, 
        string tableName, 
        string selectorColumnName, 
        List<T> selectorValues, 
        string paramPrefix = "")
    {
        // Generate parameters
        var parameters = new DynamicParameters();
        for (int i = 0; i < selectorValues.Count; i++)
            parameters.Add($"@{paramPrefix}{selectorColumnName}_{i}", selectorValues[i]);

        // Generate query string
        var sqlBuilder = new StringBuilder($"DELETE FROM {tableName} WHERE {selectorColumnName} IN (");
        sqlBuilder.Append(string.Join(',',
            Enumerable.Range(0, selectorValues.Count).Select(x => $"@{paramPrefix}{selectorColumnName}_{x}")));
        sqlBuilder.Append(");");

        // Return
        return (sqlBuilder.ToString(), parameters);        
    }

    public virtual (string Query, DynamicParameters Parameters) GenerateBulkUpdate<T>(
        DatabaseType dbType,
        string tableName,
        List<T> rowObjects, 
        List<string> selectorColumnNames, 
        List<string> columnNamesToUpdate, 
        Dictionary<string, Func<T, object>> calculatedProperties = null,
        bool useTransaction = true,
        string paramPrefix = "")
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
        var sqlBuilder = new StringBuilder();
        if (useTransaction)
            sqlBuilder.Append(TransactionOpen + Environment.NewLine);
        var parameters = new DynamicParameters();
        for (int i = 0; i < rowObjects.Count; i++)
        {
            sqlBuilder.Append($"UPDATE {tableName} SET ");
            
            // Handle plain properties
            foreach (var columnName in columnNamesToUpdate)
            {
                object value = GetPropertyValue(columnName, rowObjects[i], calculatedProperties);
                parameters.Add($"@{paramPrefix}{columnName}_{i}", value);
                sqlBuilder.Append($"{columnName} = @{paramPrefix}{columnName}_{i}, ");
            }

            // Remove trailing comma on SETs & add WHERE
            sqlBuilder.Remove(sqlBuilder.Length - 2, 2);
            sqlBuilder.AppendLine($" WHERE {string.Join(" AND ", selectorColumnNames.Select(p => $"{p} = @{paramPrefix}{p}_{i}"))};");
            foreach (var propertyName in selectorColumnNames)
            {
                var value = rowObjects[i].GetType().GetProperty(propertyName).GetValue(rowObjects[i]);
                parameters.Add($"@{paramPrefix}{propertyName}_{i}", value);
            }
        }

        // Complete the query and return
        if (useTransaction)
            sqlBuilder.Append(TransactionClose + Environment.NewLine);

        var debug = sqlBuilder.ToString();
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
    protected object GetPropertyValue<T>(
        string propertyName,
        T item,
        Dictionary<string, Func<T, object>>? calculatedProperties)
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
