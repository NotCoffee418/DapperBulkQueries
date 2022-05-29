# DapperBulkQueries

[![Nuget](https://img.shields.io/nuget/v/DapperBulkQueries.Npgsql?style=for-the-badge "Nuget")](https://www.nuget.org/packages/DapperBulkQueries.Npgsql/)

Unofficial implementation for executing bulk INSERT, UPDATE and DELETE queries with Dapper for PostgreSQL.

## Getting Started

### Install the NuGet package

You can find the nuget package in the visual studio package manager or [here](https://www.nuget.org/packages/DapperBulkQueries.Npgsql/).

In a Usings.cs file, add the following to enable the package:

```csharp
// Add this to Usings.cs
global using DapperBulkQueries.Npgsql;
// Or add the using statement to every file where you intend to use 
using DapperBulkQueries.Npgsql;
```

### Demo model
For the purposes of this demonstration, we will start off with the following:
**SQL Table:**

```sql
CREATE TABLE TestTable (
    Id bigserial PRIMARY KEY,
    TextCol character varying,
    NumberCol numeric,
    BoolCol boolean);
```

**Corresponding class:**

```csharp
public class TestTable
{
    public long Id { get; set; }
    public string TextCol { get; set; }
    public decimal NumberCol { get; set; }
    public bool BoolCol { get; set; }
}
```
I'll also assume you have a method to create and open an Npgsql connection.

### Bulk Insert


```csharp
// First we generate some data that we intend to insert.
// Optionally without specifying the ID as the `bigserial` datatype takes care of that.
List<TestTable> insertData = new()
{
    new() {
        TextCol = "aaa",
        NumberCol = 1.23m,
        BoolCol = true },
    new() {
        TextCol = "bbb",
        NumberCol = 4.56m,
        BoolCol = false },
    new() {
        TextCol = "ccc",
        NumberCol = 7m,
        BoolCol = true }
};

// We need to manually specify the column which we would like to insert
List<string> relevantColumns = new() { "TextCol", "NumberCol", "BoolCol" };

// Then we need to specify the name of the table we're inserting to
string tableName = "TestData";

// And finally we can execute the query like so:
NpgsqlConnection conn = await GetOpenConnection();
await conn.ExecuteBulkInsertAsync(
    tableName,
    insertData,
    relevantColumns);
```

### Bulk Delete
```csharp
// When bulk deleting we have a single selector column, 
// and anything containing any of the specified values will be deleted.
var valuesOfRowsToDelete = new List<string>() { "aaa", "ccc" };
await conn.ExecuteBulkDeleteAsync(
    tableName, 
    "TextCol", // Selector column
    valuesOfRowsToDelete);

```

### Bulk Update
```csharp
// Updated version of first and second
var updateData = new List<TestTable>()
{
    new TestTable() { 
        Id = 1, // Explicitly add ID, since we'll be filtering on this
        TextCol = "Updated first", 
        NumberCol = 5, 
        BoolCol = true },
    new TestTable() { 
        Id = 2,
        TextCol = "Updated second",
        NumberCol = 6, 
        BoolCol = false }
};

// With updating we can have multiple selectors. 
// Meaning, all selector values must match before a row is affected
// eg. Update WHERE Id AND BoolCol match
var selectors = new List<string>() { "Id", "BoolCol" };

// We also need to define which properties should be updated.
var propertiesToUpdate = new List<string>() { "TextCol", "NumberCol" };

// Finally we can execute the update like so
await conn.ExecuteBulkUpdateAsync(
    tableName,
    updateData,
    selectors,
    propertiesToUpdate
);
```

### Calculated Properties
You may not always want the exact property value to be taken from the class.  
For instance, if you have a property, which is a class containing an ID which references another table.  
A solution for this is provided in this package through calculated properties.

Let's redefine our TestTable class to contain such a property
```csharp
public class AnotherTable
{
    public long Id { get; set; }
    public string Name { get; set; }
}
public class TestTable
{
    public long Id { get; set; }
    public bool BoolCol { get; set; }
    
    public AnotherTable Another { get; set; }
}
```

To insert this we can make use of calculated properties, which take the shape of 
```csharp
// Dictionary key string: property name
// Function input T: in the example would be an instance of TestTable
// Function output object: the value that should be inserted in the database
Dictionary<string, Func<T, object>>
```

These override reading plain class properties if a property has a column name in the dictionary.  
**NOTE:** Properties should still also be defined in the propertyNames list for it to be included.

Example usage:
```csharp
List<string> relevantColumns = new() { 
    "TextCol", "NumberCol", "AnotherId" };

// Then we need to specify the name of the table we're inserting to
string tableName = "TestData";

// Define the calculated properties dictionary
Dictionary<string, Func<TestTable, object>> calculatedProperties = new()
{
    { "AnotherId", t => t.Another.Id }
};

// And finally we can execute the query like so:
NpgsqlConnection conn = await GetOpenConnection();
await conn.ExecuteBulkInsertAsync(
    tableName,
    insertData,
    relevantColumns,
    calculatedProperties);
```