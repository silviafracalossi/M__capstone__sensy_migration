
import java.sql.*;

public class POSDataLoader {
    Statement stmt;

    // Dropping the schema every time to avoid conflicts with old records //TODO remove this
    public POSDataLoader(Statement stmt) throws Exception {
        this.stmt = stmt;
        String drop_db_sql = "DROP SCHEMA IF EXISTS public CASCADE;";
        stmt.executeUpdate(drop_db_sql);
        System.out.println("[DROP] Schema");

        String create_db_sql = "CREATE SCHEMA public;";
        stmt.executeUpdate(create_db_sql);
        System.out.println("[CREATION] Schema");
    }

    // Creating the Accounts table, including the PK sequence
    public Boolean createAccountsTable() throws Exception {

        String accounts_id_sequence_sql = "CREATE SEQUENCE accounts_id_seq" +
                "    INCREMENT 1" +
                "    START 1" +
                "    MINVALUE 1" +
                "    MAXVALUE 9223372036854775807" +
                "    CACHE 1;";

        String accounts_table_sql = "CREATE TABLE accounts " +
                "(" +
                "    id bigint NOT NULL DEFAULT nextval('accounts_id_seq'::regclass)," +
                "    first_name character varying(128) COLLATE pg_catalog.\"default\"," +
                "    last_name character varying(128) COLLATE pg_catalog.\"default\"," +
                "    email character varying(128) COLLATE pg_catalog.\"default\"," +
                "    password bytea NOT NULL," +
                "    account_type integer NOT NULL," +
                "    code integer," +
                "    time_registered timestamp with time zone NOT NULL," +
                "    time_last_login timestamp with time zone NOT NULL," +
                "    guest_login_code bytea," +
                "    CONSTRAINT accounts_pkey PRIMARY KEY (id)," +
                "    CONSTRAINT accounts_code_unique UNIQUE (code)," +
                "    CONSTRAINT accounts_email_unique UNIQUE (email)" +
                ")";

        if (stmt.executeUpdate(accounts_id_sequence_sql) == 0) {
            System.out.println("[CREATION] Sequence: Accounts primary key");
            if (stmt.executeUpdate(accounts_table_sql) == 0) {
                System.out.println("[CREATION] Table: Accounts");
                return true;
            }
        }
        return false;
    }

    // Inserting the accounts provided by the H2 database
    public Boolean insertAccounts(ResultSet h2_accounts) throws Exception {
        if (h2_accounts != null) {
            String insert_sql = "INSERT INTO public.accounts (" +
                    "id, first_name, last_name, email, password, account_type, code, " +
                    "time_registered, time_last_login, guest_login_code)" +
                    " VALUES ";

            String row_insertion_sql = "";
            Boolean first = true;
            int count = 0;

            // Iterating through all the accounts retrieved by H2
            while (h2_accounts.next()) {

                // Checking first to add delimiter between different rows
                if (first) {
                    first = false;
                } else {
                    row_insertion_sql +=", ";
                }

                // Creating the VALUES part for the current account
                row_insertion_sql += "(";
                row_insertion_sql += "'" + h2_accounts.getInt("id") + "', ";
                row_insertion_sql += "'" + h2_accounts.getString("name") + "', ";
                row_insertion_sql += "'', ";
                row_insertion_sql += "'" + h2_accounts.getString("email") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getBytes("password") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("account_type") + "', ";
                row_insertion_sql += "'" + h2_accounts.getInt("code") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getTimestamp("time_registered") + "', ";
                row_insertion_sql += "'" + h2_accounts.getTimestamp("time_last_login") + "', ";
                row_insertion_sql+= "'" + h2_accounts.getByte("guest_login_code") + "'";
                row_insertion_sql += ")";
                row_insertion_sql = row_insertion_sql.replace("''", "NULL");

                count++;
            }

            // Final sql fixes and execution of the script
            insert_sql +=row_insertion_sql + ";";
            int rows_affected = stmt.executeUpdate(insert_sql);

            // Insertion result and return
            System.out.println("[INSERTION] Accounts inserted: " +rows_affected+ "/" +count);
            return (rows_affected == count);
        }
        return false;
    }

    // Retrieving all the accounts
    public ResultSet getAllAccounts () throws Exception {
        System.out.println("Getting all accounts from PosgreSQL database...");
        String sql =  "SELECT * FROM accounts;";
        return stmt.executeQuery(sql);
    }


}
