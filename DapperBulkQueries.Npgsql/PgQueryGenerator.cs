namespace DapperBulkQueries.Npgsql;

public class PgQueryGenerator : QueryGeneratorBase 
{
    public PgQueryGenerator() : base(
        transactionOpen: "BEGIN;",
        transactionClose: "COMMIT;")
    { }
}