import "ecere"
import "EDA"

/*
 * PosgreSQL function prototypes and type declarations
 */
#include <postgresql/libpq-fe.h>

/*
 * DEG_PGPORT
 */
#include <postgresql/pg_config.h>

static enum PGResultFormat {
    Text    = 0,
    Binary  = 1
};

class PGDataSourceDriver : DataSourceDriver
{
    class_property(name) = "PostgreSQL"

    public:
        String BuildLocator(DataSource params)
        {
            return PrintString(
                "host=",        params.host,
                "port=",        params.port,
                "user=",        params.user,
                "password=",    params.pass
            );
        }

        Database OpenDatabase(const String name, CreateOptions options, DataSource dataSource)
        {
            String locatorWithDatabaseName;

            {
                String locatorForDataSource = BuildLocator(dataSource);
                locatorWithDatabaseName = PrintString(locatorForDataSource, " dbname=", name);
                delete locatorForDataSource;
            }

            // TODO obey CreateOptions

            PGconn* pgConnection = PQconnectdb(locatorWithDatabaseName);
            delete locatorWithDatabaseName;

            return PGDatabase { connection = pgConnection };
        }
}

class PGDatabase : Database
{
    private:
        PGConn* connection;

    public:
        String GetName()
        {
            return PQdb(connection);
        }

        Array<String> GetTables()
        {
            PGresult* result = PQexecParams(
                connection,
                "SELECT format('%s.%s', table_schema, table_name) FROM information_schema.tables;",
                0, null, null, null, null,
                PGResultFormat::Text
            );

            Array<String> tables {};

            {
                ExecStatusType const execStatus = PQresultStatus(result);
                switch (execStatus)
                {
                    case PGRES_TUPLES_OK: {
                        int const rowCount = PQntuples(result);
                        uint row;
                        for (row = 0; row < rowCount; ++row)
                        {
                            // Using PrintString() to copy the result value
                            tables.Add(PrintString(PQgetvalue(result, row, 0)));
                        }
                        break;
                    };
                    default: {
                        fprintf(stderr, "Could not get a list of tables from the database: %s: %s",
                                PQresStatus(execStatus), PQresultErrorMessage(result));
                        break;
                    };
                }
            }

            PQclear(result);

            return tables;
        }

        ~PGDatabase()
        {
            PQfinish(connection);
        }
}

