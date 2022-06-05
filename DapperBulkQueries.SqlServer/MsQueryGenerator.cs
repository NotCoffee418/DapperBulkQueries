namespace DapperBulkQueries.SqlServer;

public class MsQueryGenerator : QueryGeneratorBase
{
    public MsQueryGenerator() : base(
        transactionOpen: "BEGIN TRANSACTION;",
        transactionClose: "COMMIT;") { }
}
