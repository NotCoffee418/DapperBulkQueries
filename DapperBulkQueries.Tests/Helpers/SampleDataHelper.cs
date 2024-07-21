
namespace DapperBulkQueries.Tests.Helpers;

internal static class SampleDataHelper
{
    internal static List<TestTable> GetSampleTestTablesWithId1()
        => new List<TestTable>()
        {
            new()
            {
                Id = 1,
                TextCol = "aaa",
                NumberCol = 1.23m,
                BoolCol = true,
            },
            new()
            {
                Id = 2,
                TextCol = "bbb",
                NumberCol = 4.56m,
                BoolCol = false,
            },
            new()
            {
                Id = 3,
                TextCol = "ccc",
                NumberCol = 7m,
                BoolCol = true,
            }
        };

    internal static List<TestTable> GetSampleTestTablesWithoutId1()
        => new List<TestTable>()
        {
            new()
            {
                TextCol = "aaa",
                NumberCol = 1.23m,
                BoolCol = true,
            },
            new()
            {
                TextCol = "bbb",
                NumberCol = 4.56m,
                BoolCol = false,
            },
            new()
            {
                TextCol = "ccc",
                NumberCol = 7m,
                BoolCol = true,
            }
        };
}
