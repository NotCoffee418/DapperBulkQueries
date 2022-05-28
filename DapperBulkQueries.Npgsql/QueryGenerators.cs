
using System.Linq;

namespace DapperBulkQueries.Npgsql;

public class QueryGenerators
{
    public static List<(string Query, DynamicParameters Parameters)> GenerateBulkInsert<T>(
    string tableName,
    List<T> items,
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
        for (int i = 0; i < items.Count; i++)
        {
            batchCount++;
            sqlBuilder.Append("(");

            // Handle plain properties
            foreach (var propertyName in propertyNames)
            {
                var value = items[i].GetType().GetProperty(propertyName).GetValue(items[i]);
                parameters.Add($"@{propertyName}_{i}", value);
                sqlBuilder.Append($"@{propertyName}_{i},");
            }
            // Handle calculated properties
            if (calculatedProperties is not null)
                foreach ((string propertyName, Func<T, object> getValue) in calculatedProperties)
                {
                    object value = getValue(items[i]);
                    parameters.Add($"@{propertyName}_{i}", value);
                    sqlBuilder.Append($"@{propertyName}_{i},");
                }
            sqlBuilder.Remove(sqlBuilder.Length - 1, 1);
            sqlBuilder.Append("),");

            // Execute batch if batch count is exceeded and batching is enabled
            // Or if it's the final item
            if (i == items.Count - 1 || (batchCount != 0 && batchCount >= batchSize))
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

}
